import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(processName)s %(levelname)s %(message)s",
        force=True,
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("amqp").setLevel(logging.WARNING)


def handle_client_request(client_socket, message_handler):
    configure_logging()
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)
            logging.info("Received a message from the client: %s", message)

            if message[0] == message_protocol.external.MsgType.FRUIT_RECORD:
                serialized_message = message_handler.serialize_data_message(message[1])
                logging.info("Sending a message to the results queue: %s", serialized_message)
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if message[0] == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = message_handler.serialize_eof_message(message[1])
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                return
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        output_queue.close()


def handle_client_response(client_list):
    configure_logging()
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
    logging.info("Other sub-process is listening to results queue")

    # esto se envía a la cola en particular para que se sume, supongo que hay una condición para dejar de sumar? veamo 
    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            # TODO: aquí es donde se recorre la lista
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )
                logging.info("\n\nReceived a message from the results queue: %s", deserialized_message)

                if not deserialized_message: # si es que no hay resultados
                    client_index += 1
                    continue

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.FRUIT_TOP,
                    deserialized_message,
                )
                message_protocol.external.recv_msg(client_socket)
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    configure_logging()

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        cant_cores_allowed_to_use = os.process_cpu_count()
        with multiprocessing.Pool(processes=cant_cores_allowed_to_use) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        # TODO: actualmente este proceso edita la lista mientras que otro proceso podría recorrerlo (handle_client_response)=> puede generar inconsistencias
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
