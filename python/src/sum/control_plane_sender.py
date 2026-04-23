from common import middleware, message_protocol, fruit_item
import logging
import queue

class ControlPlaneSender:
    def __init__(self, MOM_HOST, SUM_CONTROL_EXCHANGE, SUM_AMOUNT):
        self.control_sender_queue = queue.Queue()
        all_routing_keys = self._build_all_sum_control_routing_keys(SUM_AMOUNT)

        self.broadcast_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
        MOM_HOST,
        SUM_CONTROL_EXCHANGE,
        all_routing_keys,
      )
        self.direct_exchanges = {
            routing_key: middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [routing_key],
            )
            for routing_key in all_routing_keys
        }

    @staticmethod
    def _build_all_sum_control_routing_keys(SUM_AMOUNT):
        return [f"{sum_id}_control_routing_key" for sum_id in range(SUM_AMOUNT)]

    
    def _enqueue_control_broadcast(self, message):
        self.control_sender_queue.put({"mode": "broadcast", "message": message})

    def _enqueue_control_direct(self, routing_key, message):
        self.control_sender_queue.put(
            {"mode": "direct", "routing_key": routing_key, "message": message}
        )

    def run(self):
        while True:
            envelope = self.control_sender_queue.get()
            message = envelope["message"]
            serialized_message = message_protocol.internal.serialize(message)

            try:
                if envelope["mode"] == "broadcast":
                    self.broadcast_exchange.send(serialized_message)
                else:
                    routing_key = envelope["routing_key"]
                    self.direct_exchanges[routing_key].send(serialized_message)
            except Exception as error:
                logging.error("Failed to send control message: %s", error)
            finally:
                self.control_sender_queue.task_done()
