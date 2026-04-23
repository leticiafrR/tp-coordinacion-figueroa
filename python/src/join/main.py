import os
import logging
import heapq
import threading
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.keep_running = True
        self._shutdown_lock = threading.Lock()
        self._shutdown_started = False

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_top_heap_by_client_id = {}
        self.messages_received_by_client_id = {}

    def _update_client_top(self, client_id, partial_top):
        client_heap = self.partial_top_heap_by_client_id.get(client_id, [])
        for fruit_name, amount in partial_top:
            item = fruit_item.FruitItem(fruit_name, int(amount))
            if len(client_heap) < TOP_SIZE:
                heapq.heappush(client_heap, item)
            elif item > client_heap[0]:
                heapq.heapreplace(client_heap, item)
        self.partial_top_heap_by_client_id[client_id] = client_heap

    def _build_final_top(self, client_id):
        client_heap = self.partial_top_heap_by_client_id.get(client_id, [])
        fruit_chunk = sorted(client_heap, reverse=True)
        return [(fi.fruit, fi.amount) for fi in fruit_chunk]

    def request_shutdown(self):
        with self._shutdown_lock:
            if self._shutdown_started:
                return
            self._shutdown_started = True
            self.keep_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception as error:
            logging.debug("Error stopping join input consumer: %s", error)

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) != 2:
                logging.error(f"Received a message with an unexpected format: {message}")
                ack()
                return

            partial_top, client_id = fields
            logging.info("Received partial top for client: %s", client_id)

            self._update_client_top(client_id, partial_top)
            received_count = self.messages_received_by_client_id.get(client_id, 0) + 1
            self.messages_received_by_client_id[client_id] = received_count

            if received_count >= AGGREGATION_AMOUNT:
                self._send_final_results(client_id)
                self._release_client_associated_resources(client_id)
            ack()
        except Exception as error:
            if not self.keep_running:
                try:
                    nack()
                except Exception:
                    pass
                return
            logging.error("Error while processing join message: %s", error)
            nack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
            return 0
        except Exception as error:
            if not self.keep_running:
                return 0
            logging.error("Error in join main loop: %s", error)
            return 1
        finally:
            self.request_shutdown()

            try:
                self.input_queue.close()
            except Exception as error:
                logging.debug("Error closing join input queue: %s", error)

            try:
                self.output_queue.close()
            except Exception as error:
                logging.debug("Error closing join output queue: %s", error)

    def _release_client_associated_resources(self, client_id):
        del self.messages_received_by_client_id[client_id]
        del self.partial_top_heap_by_client_id[client_id]

    def _send_final_results(self, client_id):
        final_message = [self._build_final_top(client_id), client_id]
        logging.info("Sending final top for client: %s", client_id)
        self.output_queue.send(message_protocol.internal.serialize(final_message))

def main():
    assert TOP_SIZE > 0, "TOP_SIZE must be a positive integer"
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def _handle_sigterm(signum, frame):
        del signum, frame
        logging.info("SIGTERM received, starting graceful shutdown")
        join_filter.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return join_filter.start()




if __name__ == "__main__":
    main()
