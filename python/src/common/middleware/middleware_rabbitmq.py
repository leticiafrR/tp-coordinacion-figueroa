import functools
import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

from pika.exceptions import (
    AMQPConnectionError,
    AMQPError,
    ChannelClosed,
    ChannelClosedByBroker,
    ChannelClosedByClient,
    ChannelWrongStateError,
    ConnectionClosed,
    ConnectionClosedByBroker,
    ConnectionClosedByClient,
    ConnectionWrongStateError,
    StreamLostError,
)
def call_function_with_error_mapping(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except (ChannelClosed, ChannelClosedByBroker, ChannelClosedByClient, ChannelWrongStateError,
            ConnectionClosed, ConnectionClosedByBroker, ConnectionClosedByClient, ConnectionWrongStateError) as error:
        raise MessageMiddlewareCloseError("RabbitMQ channel or connection was closed") from error
    except (AMQPConnectionError, StreamLostError) as error:
        raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from error
    except AMQPError as error:
        raise MessageMiddlewareMessageError(
            f"Failed to execute RabbitMQ operation: {str(error)}"
        ) from error

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        call_function_with_error_mapping(self.__stablish_connection, host, queue_name)

    def __getattribute__(self, name):
        """Todo método público del middleware se llama a través del mapeo de errores (no incluye el constructor)"""
        attribute = object.__getattribute__(self, name)
        if name.startswith("_") or not callable(attribute):
            return attribute

        @functools.wraps(attribute)
        def wrapped(*args, **kwargs):
            return call_function_with_error_mapping(attribute, *args, **kwargs)

        return wrapped


    def __stablish_connection(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

    def __reserve_receiver_resources(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: call_function_with_error_mapping(lambda: ch.basic_ack(delivery_tag=method.delivery_tag)),
                lambda: call_function_with_error_mapping(lambda: ch.basic_nack(delivery_tag=method.delivery_tag)),
            )

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)

    def start_consuming(self, on_message_callback):
        self.reserve_receiver_resources(on_message_callback)
        self.channel.start_consuming()

    def reserve_receiver_resources(self, on_message_callback):
        self.__reserve_receiver_resources(on_message_callback)

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        if getattr(self, "channel", None) and self.channel.is_open:
            self.channel.close()
        if getattr(self, "connection", None) and self.connection.is_open:
            self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys, channel=None):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._owns_connection = channel is None
        if channel is None:
            call_function_with_error_mapping(self.__stablish_connection, host, exchange_name)
        else:
            self.channel = channel
            self.connection = getattr(channel, "connection", None)
        call_function_with_error_mapping(self.__configure_channel, exchange_name)

    def __getattribute__(self, name):
        """Todo método público del middleware se llama a través del mapeo de errores (no incluye el constructor)"""
        attribute = object.__getattribute__(self, name)
        if name.startswith("_") or not callable(attribute):
            return attribute

        @functools.wraps(attribute)
        def wrapped(*args, **kwargs):
            return call_function_with_error_mapping(attribute, *args, **kwargs)

        return wrapped

    def start_consuming(self, on_message_callback):
        self.reserve_receiver_resources(on_message_callback)
        self.channel.start_consuming()

    def reserve_receiver_resources(self, on_message_callback):
        self.__reserve_receiver_resources(on_message_callback)

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        for routing_key in self.routing_keys:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
            )

    def close(self):
        if self._owns_connection:
            connection = getattr(self, "connection", None)
            if connection and connection.is_open:
                connection.close()
    
    def __stablish_connection(self, host, exchange_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

    def __configure_channel(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    def __bind_all_routing_keys(self ):
        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

    def __reserve_receiver_resources(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: call_function_with_error_mapping(lambda: ch.basic_ack(delivery_tag=method.delivery_tag)),
                lambda: call_function_with_error_mapping(lambda: ch.basic_nack(delivery_tag=method.delivery_tag)),
            )
        result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
        self.queue_name = result.method.queue
        self.__bind_all_routing_keys()
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)