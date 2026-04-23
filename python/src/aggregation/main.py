import os
import logging
import heapq
import threading
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.keep_running = True
        self._shutdown_lock = threading.Lock()
        self._shutdown_started = False

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_items_by_client_id = {}
        self.eof_count_by_client_id = {}

    def _process_data(self, fruit, amount, client_id):
        logging.info("* Processing data message with fruit: %s and amount: %d from the client: %s", fruit, amount, client_id)
        client_fruit_items = self.fruit_items_by_client_id.get(client_id, {})
        current_fruit_item = client_fruit_items.get(fruit, fruit_item.FruitItem(fruit, 0))
        updated_fruit_item = current_fruit_item + fruit_item.FruitItem(fruit, amount)
        client_fruit_items[fruit] = updated_fruit_item
        self.fruit_items_by_client_id[client_id] = client_fruit_items
        logging.info(
            "* Updated fruit %s amount to: %d for client: %s",
            updated_fruit_item.fruit,
            updated_fruit_item.amount,
            client_id,
        )

    def _should_calculate_top(self, client_id):
        received_eof_count = self.eof_count_by_client_id.get(client_id, 0) + 1
        self.eof_count_by_client_id[client_id] = received_eof_count
        logging.info(
            "Received EOF from client: %s (%d/%d)",
            client_id,
            received_eof_count,
            SUM_AMOUNT,
        )
        return received_eof_count >= SUM_AMOUNT

    def _process_eof(self, client_id):
        if not self._should_calculate_top(client_id):
            return
        client_fruit_items = self.fruit_items_by_client_id.get(client_id, {})
        list_sending = [self._find_top_fruits(client_fruit_items), client_id]
        logging.info("* The message is "+str(list_sending))
        self.output_queue.send(
            message_protocol.internal.serialize(list_sending)
        )
        if client_id in self.eof_count_by_client_id:
            del self.eof_count_by_client_id[client_id]
        if client_id in self.fruit_items_by_client_id:
            del self.fruit_items_by_client_id[client_id]


    def _find_top_fruits(self, client_fruit_items):
        top_heap = []
        for item in client_fruit_items.values():
            if len(top_heap) < TOP_SIZE:
                heapq.heappush(top_heap, item)
            elif item > top_heap[0]:
                heapq.heapreplace(top_heap, item)

        fruit_chunk = sorted(top_heap, reverse=True)
        logging.info(f"Top fruits for client: {fruit_chunk}")
        return [(fi.fruit, fi.amount) for fi in fruit_chunk]

    def request_shutdown(self):
        with self._shutdown_lock:
            if self._shutdown_started:
                return
            self._shutdown_started = True
            self.keep_running = False

        try:
            self.input_exchange.stop_consuming()
        except Exception as error:
            logging.debug("Error stopping aggregation input consumer: %s", error)

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Process message")
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            elif len(fields) == 1:
                self._process_eof(*fields)
            else:
                logging.error(f"Received a message with an unexpected format: {message}")
            ack()
        except Exception as error:
            if not self.keep_running:
                try:
                    nack()
                except Exception:
                    pass
                return
            logging.error("Error while processing aggregation message: %s", error)
            nack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
            return 0
        except Exception as error:
            if not self.keep_running:
                return 0
            logging.error("Error in aggregation main loop: %s", error)
            return 1
        finally:
            self.request_shutdown()

            try:
                self.input_exchange.close()
            except Exception as error:
                logging.debug("Error closing aggregation input exchange: %s", error)

            try:
                self.output_queue.close()
            except Exception as error:
                logging.debug("Error closing aggregation output queue: %s", error)

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def _handle_sigterm(signum, frame):
        del signum, frame
        logging.info("SIGTERM received, starting graceful shutdown")
        aggregation_filter.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return aggregation_filter.start()


if __name__ == "__main__":
    main()
