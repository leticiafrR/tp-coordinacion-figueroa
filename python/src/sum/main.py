import os
import logging
import hashlib
import threading
import queue
from transaction.transactions import TransactionsMonitor
from transaction.active_transactions import MastersRoutingKeyByTransactionId
from digest import DigestPool
from control_plane_sender import ControlPlaneSender
from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import ControlMsgType

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

        # PROPIO del control receiver
        self.control_handlers = self._make_control_handlers()

    def _make_control_handlers(self):
        return {
            ControlMsgType.COMMIT: self._process_control_commit,
            ControlMsgType.TRYING_READY: self._process_control_trying_ready,
            ControlMsgType.OK: self._process_control_ok,
        }

    def _maybe_broadcast_ok(self, transaction_id):
        if self.transactions_monitor.digestion_complete(transaction_id):
            self.control_plane_sender._enqueue_control_broadcast(
                message_protocol.internal.make_ok(transaction_id)
            )

    def _send_digest_to_aggregators(self, transaction_id):
        # esto es lógica del receiver del control plane
        current_digest = self.digest_pool.get_current_client_digest(transaction_id)
        for fruit_name, amount in current_digest.data_per_fruit.items():
            exchange_to_use_idx = self._calculate_routing_key(fruit_name, transaction_id)
            self.data_output_exchanges[exchange_to_use_idx].send(
                message_protocol.internal.serialize([fruit_name, amount, transaction_id])
            )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([transaction_id]))

        self.digest_pool.delete_client_digest(transaction_id)
        self.transactions_monitor.delete_transaction(transaction_id)
        self.masters_routing_key_by_transaction_id.delete_transaction_info(transaction_id)

    def _process_control_commit(self, message):
        transaction_id = message["transaction_id"]
        master_routing_key = message["master_routing_key"]
        self.masters_routing_key_by_transaction_id.register_transaction_master_routing_key(
            transaction_id,
            master_routing_key,
        )

        current_digest = self.digest_pool.get_current_client_digest(transaction_id)
        self.control_plane_sender._enqueue_control_direct(
            master_routing_key,
            message_protocol.internal.make_trying_ready(
                transaction_id=transaction_id,
                amount_fruits_processed=current_digest.cant_data_processed,
            ),
        )

    def _process_control_trying_ready(self, message):
        transaction_id = message["transaction_id"]
        amount_fruits_processed = int(message["amount_fruits_processed"])

        added = self.transactions_monitor.add_processed_data_count(
            amount_fruits_processed,
            transaction_id,
        )
        if not added:
            logging.error(
                "Received TryingReady for an unknown transaction: %s", transaction_id
            )
            return

        self._maybe_broadcast_ok(transaction_id)

    def _process_control_ok(self, message):
        transaction_id = message["transaction_id"]
        self._send_digest_to_aggregators(transaction_id)

    def _process_control_message(self, message, ack, nack):
        try:
            decoded_message = message_protocol.internal.deserialize(message)
            message_type = message_protocol.internal.get_control_message_type(
                decoded_message
            )

            if message_type is None:
                logging.error("Received message without valid control type: %s", decoded_message)
                nack()
                return

            handler = self.control_handlers.get(message_type)
            if handler:
                handler(decoded_message)
            else:
                logging.error("Unknown control message type: %s", message_type)
            
            ack()
        except Exception as error:
            logging.error("Error while processing control message: %s", error)
            nack()

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
        self._maybe_broadcast_ok(client_id)

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

    def _run_control_plane_receiver(self):
        control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [SUM_CONTROL_ROUTING_KEY],
        )
        control_exchange.start_consuming(self._process_control_message)

    def start(self):
        self.control_sender_thread = threading.Thread(
            target=self.control_plane_sender.run,
            daemon=True,
            name=f"sum-{ID}-control-sender",
        )
        self.control_sender_thread.start()

        self.control_receiver_thread = threading.Thread(
            target=self._run_control_plane_receiver,
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
