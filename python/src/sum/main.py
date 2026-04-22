import os
import logging
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_CONTROL_ROUTING_KEY = f"{SUM_CONTROL_EXCHANGE}_ALL"

class SumFilter:
    def __init__(self):
        self.data_queue = None
        self.sum_control_exchange = None
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client_id_by_fruit = {}

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            [client_id] = fields
            self._process_eof(client_id)
        else:
            logging.error(f"Received a control message with an unexpected format: {message}")
        ack()

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            [client_id, total_serialized_data_messages] = fields
            logging.info(
                "Received EOF from client %s with total serialized data messages: %s",
                client_id,
                total_serialized_data_messages,
            )
            self._broadcast_eof_to_other_sums(client_id)
        else:
            logging.error(f"Received a message with an unexpected format: {message}")
        ack()

    def start(self):
        with middleware.SharedChannelAdapter(MOM_HOST) as shared_adapter:
            self.data_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST,
                INPUT_QUEUE,
                shared_adapter=shared_adapter,
            )
            self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [SUM_CONTROL_ROUTING_KEY],
                shared_adapter=shared_adapter,
            )
            self.data_queue.start_consuming(self.process_data_message)
            self.sum_control_exchange.start_consuming(self.process_control_message)


    def _process_data(self, fruit_name, amount, client_id):
        if client_id not in self.amount_by_client_id_by_fruit:
            self.amount_by_client_id_by_fruit[client_id] = {}
        if fruit_name not in self.amount_by_client_id_by_fruit[client_id]:
            self.amount_by_client_id_by_fruit[client_id][fruit_name] = fruit_item.FruitItem(fruit_name, int(amount))
            return
        new_fruit_addition = fruit_item.FruitItem(fruit_name, int(amount))
        self.amount_by_client_id_by_fruit[client_id][fruit_name] = self.amount_by_client_id_by_fruit[client_id][fruit_name] + new_fruit_addition 

    def _calculate_routing_key(self, fruit_name, client_id):
        assert AGGREGATION_AMOUNT > 0, "AGGREGATION_AMOUNT must be greater than 0 to calculate routing key"
        key = f"{fruit_name}:{client_id}"
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        hash_value = int.from_bytes(digest[:8], byteorder="big", signed=False)
        return hash_value % AGGREGATION_AMOUNT


    def _share_client_fruit_sums_one_by_one_to_aggs(self, client_id):
        fruit_items_by_client_id = self.amount_by_client_id_by_fruit.get(client_id, {})
        for final_fruit_item in fruit_items_by_client_id.values():
            logging.info("  fruit: %s, amount: %d", final_fruit_item.fruit, final_fruit_item.amount)
            exchange_to_use_idx = self._calculate_routing_key(final_fruit_item.fruit, client_id)
            self.data_output_exchanges[exchange_to_use_idx].send(
                message_protocol.internal.serialize(
                    [final_fruit_item.fruit, final_fruit_item.amount, client_id]
                )
            )
            logging.info(f"   Sending to {self.data_output_exchanges[exchange_to_use_idx].exchange_name}")

    def _process_eof(self, client_id):
        self._share_client_fruit_sums_one_by_one_to_aggs(client_id)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _broadcast_eof_to_other_sums(self, client_id):
        if self.sum_control_exchange is None:
            logging.error("Cannot broadcast EOF: sum control exchange is not initialized")
            return
        self.sum_control_exchange.send(
            message_protocol.internal.serialize([client_id])
        )


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
