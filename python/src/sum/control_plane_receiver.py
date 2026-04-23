import logging

from common import middleware, message_protocol
from common.message_protocol.internal import ControlMsgType


class ControlPlaneReceiver:
    def __init__(
        self,
        mom_host,
        sum_control_exchange,
        sum_control_routing_key,
        control_plane_sender,
        transactions_monitor,
        digest_pool,
        masters_routing_key_by_transaction_id,
        data_output_exchanges,
        calculate_routing_key,
    ):
        self.control_plane_sender = control_plane_sender
        self.transactions_monitor = transactions_monitor
        self.digest_pool = digest_pool
        self.masters_routing_key_by_transaction_id = masters_routing_key_by_transaction_id
        self.data_output_exchanges = data_output_exchanges
        self.calculate_routing_key = calculate_routing_key

        self.control_handlers = {
            ControlMsgType.COMMIT: self._process_control_commit,
            ControlMsgType.TRYING_READY: self._process_control_trying_ready,
            ControlMsgType.OK: self._process_control_ok,
        }

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            mom_host,
            sum_control_exchange,
            [sum_control_routing_key],
        )

    def run(self):
        self.control_exchange.start_consuming(self._process_control_message)

    def stop(self):
        self.control_exchange.stop_consuming()

    def close(self):
        self.control_exchange.close()

    def _process_control_message(self, message, ack, nack):
        try:
            decoded_message = message_protocol.internal.deserialize(message)
            if not isinstance(decoded_message, list) or len(decoded_message) == 0:
                logging.error("Received malformed control message: %s", decoded_message)
                nack()
                return

            message_type = message_protocol.internal.get_control_message_type(decoded_message)
            normalized_message = message_protocol.internal.parse_control_message(decoded_message)
            if normalized_message is None:
                logging.error("Received malformed control message: %s", decoded_message)
                nack()
                return

            handler = self.control_handlers.get(message_type)
            if handler:
                handler(normalized_message)
            else:
                logging.error("Unknown control message type: %s", message_type)
            ack()
        except Exception as error:
            logging.error("Error while processing control message: %s", error)
            nack()

    def maybe_broadcast_ok(self, transaction_id):
        if self.transactions_monitor.digestion_complete(transaction_id):
            self.control_plane_sender._enqueue_control_broadcast(
                message_protocol.internal.make_ok(transaction_id)
            )

    def _send_digest_to_aggregators(self, transaction_id):
        current_digest = self.digest_pool.get_current_client_digest(transaction_id)
        for fruit_name, amount in current_digest.data_per_fruit.items():
            exchange_to_use_idx = self.calculate_routing_key(fruit_name, transaction_id)
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

        self.maybe_broadcast_ok(transaction_id)

    def _process_control_ok(self, message):
        transaction_id = message["transaction_id"]
        self._send_digest_to_aggregators(transaction_id)
