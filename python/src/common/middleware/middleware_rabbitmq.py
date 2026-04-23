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


def request_stop_consuming_threadsafe(connection, channel):
    if connection and connection.is_open and channel and channel.is_open:
        connection.add_callback_threadsafe(channel.stop_consuming)

class SharedChannelAdapter:
    def __init__(self, host):
        self.host = host
        self.connection = None
        self.channel = None
        self._registered_count = 0
        self._ready_count = 0
        self._setup_functions = []

    def __enter__(self):
        """Inicializa los recursos al entrar en el bloque 'with'."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Libera los recursos garantizando el cierre de la conexión TCP."""
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()

    def register(self):
        self._registered_count += 1

    def notify_ready(self, setup_func):
        self._setup_functions.append(setup_func)
        self._ready_count += 1
        # Solo arranca cuando todos los registrados llamaron a start_consuming
        if self._ready_count == self._registered_count:
            if self.channel is None:
                raise MessageMiddlewareCloseError("Cannot start consuming: Channel is not initialized")
            for func in self._setup_functions:
                func(self.channel)
            call_function_with_error_mapping(self.channel.start_consuming)

    def execute_operation(self, operation):
        if self.channel and self.channel.is_open:
            return operation(self.channel)
        raise MessageMiddlewareCloseError("Cannot execute operation: Channel is closed")
    
class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name, shared_adapter=None):
        self.queue_name = queue_name
        self.shared_adapter = shared_adapter
        self.channel = None
        
        if self.shared_adapter:
            self.channel = self.shared_adapter.channel
            self.connection = self.shared_adapter.connection
            self.shared_adapter.register()
            self.shared_adapter.execute_operation(lambda ch: ch.queue_declare(queue=self.queue_name))
        else:
            call_function_with_error_mapping(self.__establish_connection, host)

    
    def __getattribute__(self, name):
        """Todo método público del middleware se llama a través del mapeo de errores (no incluye el constructor)"""
        attribute = object.__getattribute__(self, name)
        if name.startswith("_") or not callable(attribute):
            return attribute

        @functools.wraps(attribute)
        def wrapped(*args, **kwargs):
            return call_function_with_error_mapping(attribute, *args, **kwargs)
        return wrapped

    def __establish_connection(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def _get_consume_setup(self, on_message_callback):
        #maybe retornar una lambda
        def setup(ch):
            def callback(ch, method, properties, body):
                on_message_callback(
                    body,
                    lambda: call_function_with_error_mapping(ch.basic_ack, delivery_tag=method.delivery_tag),
                    lambda: call_function_with_error_mapping(ch.basic_nack, delivery_tag=method.delivery_tag)
                )
            ch.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
        return setup

    def start_consuming(self, on_message_callback):
        setup_func = self._get_consume_setup(on_message_callback)
        if self.shared_adapter:
            self.shared_adapter.notify_ready(setup_func)
        elif self.channel:
            setup_func(self.channel)
            self.channel.start_consuming()

    def stop_consuming(self):
        if self.shared_adapter:
            request_stop_consuming_threadsafe(
                self.shared_adapter.connection,
                self.shared_adapter.channel,
            )
        else:
            request_stop_consuming_threadsafe(self.connection, self.channel)

    def send(self, message):
        operation = lambda ch: ch.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        if self.shared_adapter:
            self.shared_adapter.execute_operation(operation)
        elif self.channel:
            operation(self.channel)

    def close(self):
        if not self.shared_adapter:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys, shared_adapter=None):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.shared_adapter = shared_adapter
        self.channel = None
        self.connection = None
        
        if self.shared_adapter:
            self.channel = self.shared_adapter.channel
            self.connection = self.shared_adapter.connection
            self.shared_adapter.register()
            self.shared_adapter.execute_operation(lambda ch: ch.exchange_declare(exchange=self.exchange_name, exchange_type='direct'))
        else:
            call_function_with_error_mapping(self.__establish_connection, host)

    def __getattribute__(self, name):
        """Todo método público del middleware se llama a través del mapeo de errores (no incluye el constructor)"""
        attribute = object.__getattribute__(self, name)
        if name.startswith("_") or not callable(attribute):
            return attribute

        @functools.wraps(attribute)
        def wrapped(*args, **kwargs):
            return call_function_with_error_mapping(attribute, *args, **kwargs)

        return wrapped

    def __establish_connection(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()  
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def _get_consume_setup(self, on_message_callback):
        def setup(ch):
            result = ch.queue_declare(queue='', exclusive=True, auto_delete=True)
            temp_queue = result.method.queue
            for key in self.routing_keys:
                ch.queue_bind(exchange=self.exchange_name, queue=temp_queue, routing_key=key)
            
            def callback(ch, method, properties, body):
                on_message_callback(
                    body,
                    lambda: call_function_with_error_mapping(ch.basic_ack, delivery_tag=method.delivery_tag),
                    lambda: call_function_with_error_mapping(ch.basic_nack, delivery_tag=method.delivery_tag)
                )
            ch.basic_consume(queue=temp_queue, on_message_callback=callback, auto_ack=False)
        return setup

    def start_consuming(self, on_message_callback):
        setup_func = self._get_consume_setup(on_message_callback)
        if self.shared_adapter:
            self.shared_adapter.notify_ready(setup_func)
        elif self.channel:
            setup_func(self.channel)
            self.channel.start_consuming()

    def stop_consuming(self):
        if self.shared_adapter:
            request_stop_consuming_threadsafe(
                self.shared_adapter.connection,
                self.shared_adapter.channel,
            )
        else:
            request_stop_consuming_threadsafe(self.connection, self.channel)

    def send(self, message):
        def operation(ch):
            for key in self.routing_keys:
                ch.basic_publish(exchange=self.exchange_name, routing_key=key, body=message)
        
        if self.shared_adapter:
            self.shared_adapter.execute_operation(operation)
        elif self.channel:
            operation(self.channel)

    def close(self):
        if not self.shared_adapter:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
