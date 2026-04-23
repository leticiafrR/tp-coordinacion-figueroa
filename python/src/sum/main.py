import os
import logging
import hashlib
import threading
import queue

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


class VotationStatus:
    def __init__(self, expected_processed_data_count):
        self.expected_processed_data_count = int(expected_processed_data_count)
        self.current_processed_data_count = 0
        self.ok_broadcasted = False


class VotationsMonitor:
    def __init__(self):
        self.votations = {}
        self.mutex = threading.Lock()

    def regist_new_votation(self, client_id, expected_data_count):
        with self.mutex:
            if client_id not in self.votations:
                self.votations[client_id] = VotationStatus(expected_data_count)
            else:
                self.votations[client_id].expected_processed_data_count = int(
                    expected_data_count
                )

    def add_processed_data_count(self, processed_data_count, votation_id):
        with self.mutex:
            status = self.votations.get(votation_id)
            if status is None:
                return False
            status.current_processed_data_count += int(processed_data_count)
            return True

    def digestion_complete(self, votation_id):
        with self.mutex:
            status = self.votations.get(votation_id)
            if status is None:
                return False
            return (
                status.current_processed_data_count
                >= status.expected_processed_data_count
            )

    def mark_ok_as_broadcasted(self, votation_id):
        with self.mutex:
            status = self.votations.get(votation_id)
            if status is None:
                return False
            if status.ok_broadcasted:
                return False
            if (
                status.current_processed_data_count
                < status.expected_processed_data_count
            ):
                return False
            status.ok_broadcasted = True
            return True

    def delete_votation(self, votation_id):
        with self.mutex:
            self.votations.pop(votation_id, None)


class ClientDigest:
    def __init__(self):
        self.cant_data_processed = 0
        self.data_per_fruit = {}

    def digest(self, fruit, amount):
        self.cant_data_processed += 1
        current_amount = self.data_per_fruit.get(fruit, 0)
        self.data_per_fruit[fruit] = current_amount + int(amount)


class DigestPool:
    def __init__(self):
        self.pool = {}
        self.mutex = threading.Lock()

    def digest_client_data(self, client_id, fruit, amount):
        with self.mutex:
            client_digest = self.pool.get(client_id)
            if client_digest is None:
                client_digest = ClientDigest()
                self.pool[client_id] = client_digest
            client_digest.digest(fruit, amount)

    def get_current_client_digest(self, client_id):
        with self.mutex:
            client_digest = self.pool.get(client_id)
            if client_digest is None:
                return ClientDigest()
            snapshot = ClientDigest()
            snapshot.cant_data_processed = client_digest.cant_data_processed
            snapshot.data_per_fruit = dict(client_digest.data_per_fruit)
            return snapshot

    def delete_client_digest(self, client_id):
        with self.mutex:
            self.pool.pop(client_id, None)


class MastersRoutingKeyByVotationID:
    def __init__(self):
        self.routing_key_by_votation_id = {}
        self.mutex = threading.Lock()

    def regist_votation_master_routing_key(self, votation_id, routing_key):
        with self.mutex:
            self.routing_key_by_votation_id[votation_id] = routing_key

    def look_master_routing_key(self, votation_id):
        with self.mutex:
            return self.routing_key_by_votation_id.get(votation_id)

    def delete_votation(self, votation_id):
        with self.mutex:
            self.routing_key_by_votation_id.pop(votation_id, None)

class SumFilter:
    def __init__(self):
        self.control_sender_queue = queue.Queue()
        self.votations_monitor = VotationsMonitor()
        self.digest_pool = DigestPool()
        self.masters_routing_key_by_votation_id = MastersRoutingKeyByVotationID()
        self.sender_thread = None
        self.data_plane_thread = None
        self.control_receiver_thread = None

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def _build_all_sum_control_routing_keys(self):
        return [f"{sum_id}_control_routing_key" for sum_id in range(SUM_AMOUNT)]

    def _enqueue_control_broadcast(self, message):
        self.control_sender_queue.put({"mode": "broadcast", "message": message})

    def _enqueue_control_direct(self, routing_key, message):
        self.control_sender_queue.put(
            {"mode": "direct", "routing_key": routing_key, "message": message}
        )

    def _run_control_plane_sender(self):
        all_routing_keys = self._build_all_sum_control_routing_keys()
        broadcast_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            all_routing_keys,
        )
        direct_exchanges = {
            routing_key: middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [routing_key],
            )
            for routing_key in all_routing_keys
        }

        while True:
            envelope = self.control_sender_queue.get()
            message = envelope["message"]
            serialized_message = message_protocol.internal.serialize(message)

            try:
                if envelope["mode"] == "broadcast":
                    broadcast_exchange.send(serialized_message)
                else:
                    routing_key = envelope["routing_key"]
                    direct_exchanges[routing_key].send(serialized_message)
            except Exception as error:
                logging.error("Failed to send control message: %s", error)
            finally:
                self.control_sender_queue.task_done()

    def _maybe_broadcast_ok(self, votation_id):
        if self.votations_monitor.mark_ok_as_broadcasted(votation_id):
            self._enqueue_control_broadcast(
                message_protocol.internal.make_ok(votation_ID=votation_id)
            )

    def _send_digest_to_aggregators(self, votation_id):
        current_digest = self.digest_pool.get_current_client_digest(votation_id)
        for fruit_name, amount in current_digest.data_per_fruit.items():
            exchange_to_use_idx = self._calculate_routing_key(fruit_name, votation_id)
            self.data_output_exchanges[exchange_to_use_idx].send(
                message_protocol.internal.serialize([fruit_name, amount, votation_id])
            )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([votation_id]))

        self.digest_pool.delete_client_digest(votation_id)
        self.votations_monitor.delete_votation(votation_id)
        self.masters_routing_key_by_votation_id.delete_votation(votation_id)

    def _process_control_commit(self, message):
        votation_id = message["votation_ID"]
        master_routing_key = message["master_routing_key"]
        self.masters_routing_key_by_votation_id.regist_votation_master_routing_key(
            votation_id,
            master_routing_key,
        )

        current_digest = self.digest_pool.get_current_client_digest(votation_id)
        self._enqueue_control_direct(
            master_routing_key,
            message_protocol.internal.make_trying_ready(
                votation_ID=votation_id,
                amount_fruits_processed=current_digest.cant_data_processed,
            ),
        )

    def _process_control_trying_ready(self, message):
        votation_id = message["votation_ID"]
        amount_fruits_processed = int(message["amount_fruits_processed"])

        added = self.votations_monitor.add_processed_data_count(
            amount_fruits_processed,
            votation_id,
        )
        if not added:
            logging.error(
                "Received TryingReady for an unknown votation: %s", votation_id
            )
            return

        self._maybe_broadcast_ok(votation_id)

    def _process_control_ok(self, message):
        votation_id = message["votation_ID"]
        self._send_digest_to_aggregators(votation_id)

    def _process_control_message(self, message, ack, nack):
        try:
            decoded_message = message_protocol.internal.deserialize(message)
            message_type = message_protocol.internal.get_control_message_type(
                decoded_message
            )

            if message_type == message_protocol.internal.CONTROL_MSG_TYPE_COMMIT:
                self._process_control_commit(decoded_message)
            elif message_type == message_protocol.internal.CONTROL_MSG_TYPE_TRYING_READY:
                self._process_control_trying_ready(decoded_message)
            elif message_type == message_protocol.internal.CONTROL_MSG_TYPE_OK:
                self._process_control_ok(decoded_message)
            else:
                logging.error("Unexpected control message: %s", decoded_message)
            ack()
        except Exception as error:
            logging.error("Error while processing control message: %s", error)
            nack()

    def _process_data(self, fruit_name, amount, client_id):
        self.digest_pool.digest_client_data(client_id, fruit_name, int(amount))
        master_routing_key = (
            self.masters_routing_key_by_votation_id.look_master_routing_key(client_id)
        )
        if master_routing_key:
            self._enqueue_control_direct(
                master_routing_key,
                message_protocol.internal.make_trying_ready(
                    votation_ID=client_id,
                    amount_fruits_processed=1,
                ),
            )

    def _process_data_eof(self, client_id, total_serialized_data_messages):
        logging.info(
            "Received EOF from client %s with total serialized data messages: %s",
            client_id,
            total_serialized_data_messages,
        )

        self.votations_monitor.regist_new_votation(
            client_id,
            int(total_serialized_data_messages),
        )

        master_routing_key = SUM_CONTROL_ROUTING_KEY
        self.masters_routing_key_by_votation_id.regist_votation_master_routing_key(
            client_id,
            master_routing_key,
        )

        self._enqueue_control_broadcast(
            message_protocol.internal.make_commit(
                votation_ID=client_id,
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
        self.sender_thread = threading.Thread(
            target=self._run_control_plane_sender,
            daemon=True,
            name=f"sum-{ID}-control-sender",
        )
        self.sender_thread.start()

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
