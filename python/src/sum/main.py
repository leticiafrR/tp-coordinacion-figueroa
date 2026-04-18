import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"#common for all the sum filters, they use them for subscribing
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_CONTROL_ROUTING_KEY = f"{SUM_CONTROL_EXCHANGE}_ALL"

class SumFilter:
    def __init__(self):
        self.data_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [SUM_CONTROL_ROUTING_KEY],
            channel=self.data_queue.channel,
        )
        self.data_output_exchanges = []
        # logging.info("There will be creating %d exchanges with the next configuration", AGGREGATION_AMOUNT)
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            # logging.info("-> Created exchange with name: %s and routing keys: %s", AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"])
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client_id_by_fruit = {}

    def _process_data(self, fruit_name, amount, client_id):
        if client_id not in self.amount_by_client_id_by_fruit:
            self.amount_by_client_id_by_fruit[client_id] = {}
        if fruit_name not in self.amount_by_client_id_by_fruit[client_id]:
            self.amount_by_client_id_by_fruit[client_id][fruit_name] = fruit_item.FruitItem(fruit_name, int(amount))
            # logging.info(f"-> Added new fruit {fruit_name} with amount {amount}")
            return
        new_fruit_addition = fruit_item.FruitItem(fruit_name, int(amount))
        self.amount_by_client_id_by_fruit[client_id][fruit_name] = self.amount_by_client_id_by_fruit[client_id][fruit_name] + new_fruit_addition 
        # logging.info(f"-> Added fruit already registered: {self.amount_by_client_id_by_fruit[client_id][fruit_name].fruit}, new amount {self.amount_by_client_id_by_fruit[client_id][fruit_name].amount}")


    def _process_eof(self, client_id):
        logging.info(f"------------------------------------->processing EOF [{client_id}]<--------")
        # logging.info(f"Broadcasting data messages to output exchanges ({len(self.data_output_exchanges)} processes) for client_id: {client_id}")
        fruit_items_by_client_id = self.amount_by_client_id_by_fruit.get(client_id, {})
        logging.info("--->first sending the results")
        if len(fruit_items_by_client_id) == 0:
            logging.warning(f"No fruit items found for client_id: {client_id}")
        for final_fruit_item in fruit_items_by_client_id.values():
            logging.info("  fruit: %s, amount: %d", final_fruit_item.fruit, final_fruit_item.amount)
            for data_output_exchange in self.data_output_exchanges: # para el caso de un aggregatioN filter esto es equivalente a llamar una vez 
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [final_fruit_item.fruit, final_fruit_item.amount, client_id]
                    )
                )
                logging.info(f"   Sending to {data_output_exchange.exchange_name}")


        # logging.info("--->then sending the EOF")
        #quizás esto se deba de enviar a todos los sum cuando uno de los sum recibe el EOF de un worker (de forma que puedan continuar pasando los resultados ya acumulados)
        #con una instancia de sum es trivial
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _broadcast_eof_to_other_sums(self, client_id):
        self.sum_control_exchange.send(
            message_protocol.internal.serialize(["sum_eof", client_id, ID])
        )

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3 and fields[0] == "sum_eof":
            _, client_id, sender_id = fields
            logging.info(f"[{ID}]Receiving EOF from control plane \n      [client_id: {client_id} sent by sum filter with id: {sender_id}]")
            if int(sender_id) != ID:
                logging.info(f"[{ID}]   It wasn't an auto fannout. Processing Real new EOF spread by {sender_id}. Now I will process the EOF")
                self._process_eof(client_id)
        else:
            logging.error(f"Received a control message with an unexpected format: {message}")
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            client_id = fields[0]
            logging.info(f"->EOF received for client_id: {client_id}, processing it (flushing accumulated data to agg filters)")
            self._process_eof(client_id)
            logging.info(f"->EOF [{client_id}] broadcasting to other sum filters")
            self._broadcast_eof_to_other_sums(client_id)
        else:
            logging.error(f"Received a message with an unexpected format: {message}")
        ack()

    def start(self):
        self.data_queue.reserve_receiver_resources(self.process_data_messsage)
        self.sum_control_exchange.reserve_receiver_resources(self.process_control_message)
        self.data_queue.channel.start_consuming()

def log_env():
    logging.info(f"ID: {ID}")
    logging.info(f"MOM_HOST: {MOM_HOST}")
    logging.info(f"INPUT_QUEUE: {INPUT_QUEUE}")
    logging.info(f"SUM_AMOUNT: {SUM_AMOUNT}")
    logging.info(f"SUM_PREFIX: {SUM_PREFIX}")
    logging.info(f"AGGREGATION_AMOUNT: {AGGREGATION_AMOUNT}")
    logging.info(f"AGGREGATION_PREFIX: {AGGREGATION_PREFIX}")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    log_env()
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
