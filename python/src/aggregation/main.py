import os
import logging
import bisect

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
        self.fruit_top_by_client_id = {}

    def _process_data(self, fruit, amount, client_id):
        logging.info("* Processing data message with fruit: %s and amount: %d from the client: %s", fruit, amount, client_id)
        client_fruit_top = self.fruit_top_by_client_id.get(client_id, [])
        for i in range(len(client_fruit_top)):
            if client_fruit_top[i].fruit == fruit:
                client_fruit_top[i] = client_fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                self.fruit_top_by_client_id[client_id] = client_fruit_top
                logging.warning("* In some MOMENT, aggregation filter received a fruit %s (summed previously) and updated the amount to: %d", client_fruit_top[i].fruit, client_fruit_top[i].amount)
                return
        # TODO
        bisect.insort(client_fruit_top, fruit_item.FruitItem(fruit, amount))
        self.fruit_top_by_client_id[client_id] = client_fruit_top
        logging.info("* Aggregation filter received a fruit %s (not summed previously) and added it to the list of fruits with amount: %d", fruit, amount)

    def _process_eof(self, client_id):
        logging.info("Received EOF from client: %s", client_id)
        client_fruit_top = self.fruit_top_by_client_id.get(client_id, [])
        fruit_chunk = list(client_fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
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
        #TODO: arreglar esto en el caso de múltiples aggregations pues este sería el eof de uno de los agg-filter
        if client_id in self.fruit_top_by_client_id:
            del self.fruit_top_by_client_id[client_id]

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
