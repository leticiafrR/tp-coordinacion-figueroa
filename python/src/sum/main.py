import os
import logging
import hashlib
import threading
from transaction.transactions import TransactionsMonitor
from transaction.active_transactions import MastersRoutingKeyByTransactionId
from digest import DigestPool
from control_plane_sender import ControlPlaneSender
from control_plane_receiver import ControlPlaneReceiver
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_CONTROL_ROUTING_KEY = f"{ID}_control_routing_key"


class SumFilter:
    def __init__(self):
        self.control_plane_sender = ControlPlaneSender(MOM_HOST, SUM_CONTROL_EXCHANGE, SUM_AMOUNT)
        self.transactions_monitor = TransactionsMonitor()
        self.digest_pool = DigestPool()
        self.masters_routing_key_by_transaction_id = MastersRoutingKeyByTransactionId()
        self.control_sender_thread = None
        self.data_plane_thread = None
        self.control_receiver_thread = None

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_plane_receiver = ControlPlaneReceiver(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            SUM_CONTROL_ROUTING_KEY,
            self.control_plane_sender,
            self.transactions_monitor,
            self.digest_pool,
            self.masters_routing_key_by_transaction_id,
            self.data_output_exchanges,
            self._calculate_routing_key,
        )

    def _process_data(self, fruit_name, amount, client_id):
        self.digest_pool.digest_client_data(client_id, fruit_name, int(amount))
        master_routing_key = (
            self.masters_routing_key_by_transaction_id.look_master_routing_key(client_id)
        )
        if master_routing_key:
            self.control_plane_sender._enqueue_control_direct(
                master_routing_key,
                message_protocol.internal.make_trying_ready(
                    transaction_id=client_id,
                    amount_fruits_processed=1,
                ),
            )

    def _process_data_eof(self, client_id, total_serialized_data_messages):
        logging.info(
            "Received EOF from client %s with total serialized data messages: %s",
            client_id,
            total_serialized_data_messages,
        )

        self.transactions_monitor.begin_transaction(
            client_id,
            int(total_serialized_data_messages),
        )

        master_routing_key = SUM_CONTROL_ROUTING_KEY
        self.masters_routing_key_by_transaction_id.register_transaction_master_routing_key(
            client_id,
            master_routing_key,
        )

        self.control_plane_sender._enqueue_control_broadcast(
            message_protocol.internal.make_commit(
                transaction_id=client_id,
                master_routing_key=master_routing_key,
            )
        )
        self.control_plane_receiver.maybe_broadcast_ok(client_id)

    def _process_data_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            elif len(fields) == 2:
                [client_id, total_serialized_data_messages] = fields
                self._process_data_eof(client_id, total_serialized_data_messages)
            else:
                logging.error("Received a message with an unexpected format: %s", message)
            ack()
        except Exception as error:
            logging.error("Error while processing data message: %s", error)
            nack()

    def _run_data_plane_consumer(self):
        data_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        data_queue.start_consuming(self._process_data_message)

    def start(self):
        self.control_sender_thread = threading.Thread(
            target=self.control_plane_sender.run,
            daemon=True,
            name=f"sum-{ID}-control-sender",
        )
        self.control_sender_thread.start()

        self.control_receiver_thread = threading.Thread(
            target=self.control_plane_receiver.run,
            daemon=False,
            name=f"sum-{ID}-control-receiver",
        )
        self.control_receiver_thread.start()

        self.data_plane_thread = threading.Thread(
            target=self._run_data_plane_consumer,
            daemon=False,
            name=f"sum-{ID}-data-consumer",
        )
        self.data_plane_thread.start()

        self.control_receiver_thread.join()
        self.data_plane_thread.join()
        self.control_sender_thread.join()

    def _calculate_routing_key(self, fruit_name, client_id):
        assert AGGREGATION_AMOUNT > 0, "AGGREGATION_AMOUNT must be greater than 0 to calculate routing key"
        key = f"{fruit_name}:{client_id}"
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        hash_value = int.from_bytes(digest[:8], byteorder="big", signed=False)
        return hash_value % AGGREGATION_AMOUNT


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
