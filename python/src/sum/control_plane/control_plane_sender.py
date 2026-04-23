from common import middleware, message_protocol, fruit_item
import logging
import queue


class ControlPlaneSender:
    def __init__(self, MOM_HOST, SUM_CONTROL_EXCHANGE, SUM_AMOUNT):
        self.control_sender_queue = queue.Queue()
        self._stopped = False
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

    def _safe_put(self, envelope):
        if self._stopped:
            return
        try:
            self.control_sender_queue.put(envelope)
        except Exception as error:
            shutdown_exc = getattr(queue, "ShutDown", None)
            if shutdown_exc is not None and isinstance(error, shutdown_exc):
                return
            raise

    
    def _enqueue_control_broadcast(self, message):
        self._safe_put({"mode": "broadcast", "message": message})

    def _enqueue_control_direct(self, routing_key, message):
        self._safe_put(
            {"mode": "direct", "routing_key": routing_key, "message": message}
        )

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self.control_sender_queue.shutdown(immediate=True)

    def close(self):
        try:
            self.broadcast_exchange.close()
        except Exception as error:
            logging.debug("Error closing control broadcast exchange: %s", error)

        for exchange in self.direct_exchanges.values():
            try:
                exchange.close()
            except Exception as error:
                logging.debug("Error closing control direct exchange: %s", error)

    def run(self):
        while True:
            try:
                envelope = self.control_sender_queue.get()
            except Exception as error:
                shutdown_exc = getattr(queue, "ShutDown", None)
                if shutdown_exc is not None and isinstance(error, shutdown_exc):
                    break
                raise

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

        self.close()
