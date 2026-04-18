import os
import logging
import heapq

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
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_items_by_client_id = {}
        self.eof_count_by_client_id = {}
        self.completed_client_ids = set()

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

    def _process_eof(self, client_id):
        if client_id in self.completed_client_ids:
            logging.info("Ignoring duplicated EOF for completed client: %s", client_id)
            return

        received_eof_count = self.eof_count_by_client_id.get(client_id, 0) + 1
        self.eof_count_by_client_id[client_id] = received_eof_count

        logging.info(
            "Received EOF from client: %s (%d/%d)",
            client_id,
            received_eof_count,
            SUM_AMOUNT,
        )

        if received_eof_count < SUM_AMOUNT:
            return

        client_fruit_items = self.fruit_items_by_client_id.get(client_id, {})
        fruit_heap = list(client_fruit_items.values())
        heapq.heapify(fruit_heap)
        fruit_chunk = heapq.nlargest(TOP_SIZE, fruit_heap)
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        logging.info("Sending the fruit top: %s for client: %s to the results queue", fruit_top, client_id)
        list_sending = [fruit_top, client_id]
        logging.info("* The message is "+str(list_sending))
        self.output_queue.send(
            message_protocol.internal.serialize(list_sending)
        )

        self.completed_client_ids.add(client_id)
        if client_id in self.eof_count_by_client_id:
            del self.eof_count_by_client_id[client_id]
        if client_id in self.fruit_items_by_client_id:
            del self.fruit_items_by_client_id[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof(*fields)
        else:
            logging.error(f"Received a message with an unexpected format: {message}")
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
