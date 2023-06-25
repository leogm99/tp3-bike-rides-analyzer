import logging
import socket
import threading
import uuid
from time import time

from common.loader.client_handler_resources import ClientHandlerResources
from common.loader.stream_state import StreamState
from common_utils.protocol.message import Message, TRIPS, STATIONS, WEATHER
from common_utils.protocol.protocol import Protocol
from common_utils.utils import recv_n_bytes


class ClientHandler(threading.Thread):
    def __init__(self,
                 hostname,
                 client_socket_queue,
                 client_semaphore,
                 middleware_factory,
                 trips_consumer_replica_count,
                 weather_consumer_replica_count,
                 stations_consumer_replica_count,
                 ack_count,
                 stop_event):
        super().__init__()
        self._stop_event = stop_event
        self._client_socket_queue = client_socket_queue
        self._client_semaphore = client_semaphore
        self._client_socket = None

        self._client_handler_resources = ClientHandlerResources(
            hostname=hostname,
            middleware_factory=middleware_factory,
            ack_count=ack_count,
        )

        self._trips_consumer_replica_count = trips_consumer_replica_count
        self._weather_consumer_replica_count = weather_consumer_replica_count
        self._stations_consumer_replica_count = stations_consumer_replica_count

    def run(self) -> None:
        logging.info('action: client-handler-run | status: waiting clients')
        while not self._stop_event.is_set():
            self.__receive_client_socket()
            if self._client_socket is None:
                break
            logging.info('action: client-handler-run | client arrived')
            self.__process_client()
            self._client_semaphore.release()
            logging.info('action: client-handler-run | client finished')

    def __receive_client_socket(self):
        self._client_socket = self._client_socket_queue.get()

    def __process_client(self):
        # TODO: de-hardcode timeout
        self._client_socket.settimeout(30)
        client_id = ClientHandler.__get_client_id()
        stream_state = StreamState()
        try:
            self._client_handler_resources.acquire(client_id.payload.data)
            Protocol.send_message(self._client_socket.sendall, client_id)
            middleware = self._client_handler_resources.get_middleware()
            while stream_state.not_static_data_eof_received():
                self.__receive_client_message_and_publish(self._client_socket, stream_state, middleware)

            ack = Message.build_ack_message(client_id.payload.data)
            Protocol.send_message(self._client_socket.sendall, ack)

            while stream_state.not_trips_eof_received():
                self.__receive_client_message_and_publish(self._client_socket, stream_state, middleware)

            metrics_queue = self._client_handler_resources.get_metrics_queue()
            metrics = metrics_queue.get()
            Protocol.send_message(self._client_socket.sendall, metrics)
        except socket.error:
            if self._stop_event.is_set():
                logging.info(f'action: process-client | status: closing down | stop received')
        except BaseException as e:
            if self._stop_event.is_set():
                logging.info(f'action: process-client | status: closing down | stop received')
            else:
                logging.error(f'action: process-client | status: failed | reason: {e}', stack_info=True)
        finally:
            # avoid "double free"
            if not self._stop_event.is_set():
                self._client_socket.shutdown(socket.SHUT_RDWR)
                self._client_socket.close()
                self._client_socket = None
            self._client_handler_resources.release()

    def __receive_client_message_and_publish(self, client_socket, stream_state, middleware):
        message = Protocol.receive_message(lambda n: recv_n_bytes(client_socket, n))
        message.timestamp = time()
        if message.is_eof():
            self.__on_eof_threshold_reached(message.message_type, message.client_id, message.timestamp, stream_state,
                                            middleware)
        else:
            if message.is_type(TRIPS):
                replica_count = self._trips_consumer_replica_count
                send_to = middleware.send_trips
            elif message.is_type(STATIONS):
                replica_count = self._stations_consumer_replica_count
                send_to = middleware.send_stations
            elif message.is_type(WEATHER):
                replica_count = self._weather_consumer_replica_count
                send_to = middleware.send_weather
            else:
                raise ValueError("Invalid type of data received")

            routing_key = int(message.message_id) % replica_count
            raw_msg = Protocol.serialize_message(message)
            send_to(raw_msg, routing_key)

    def __on_eof_threshold_reached(self,
                                   eof_type: str,
                                   client_id: str,
                                   timestamp: float,
                                   stream_state: StreamState,
                                   middleware):
        if eof_type == STATIONS:
            replica_count = self._stations_consumer_replica_count
            send = middleware.send_stations
            stream_state.set_stations_eof()
        elif eof_type == WEATHER:
            replica_count = self._weather_consumer_replica_count
            send = middleware.send_weather
            stream_state.set_weather_eof()
        elif eof_type == TRIPS:
            replica_count = self._trips_consumer_replica_count
            send = middleware.send_trips
            stream_state.set_trips_eof()
        else:
            raise ValueError("Invalid type of data received")
        eof = Message.build_eof_message(message_type=eof_type, client_id=client_id, timestamp=timestamp)
        for i in range(replica_count):
            send(Protocol.serialize_message(eof), i)

    @staticmethod
    def __get_client_id():
        client_id = str(uuid.uuid4())  # 16 bytes
        id_msg = Message.build_id_message(client_id)
        return id_msg

    def stop(self):
        self._stop_event.set()
        # not polite
        if self._client_socket:
            self._client_socket.shutdown(socket.SHUT_RDWR)
            self._client_socket.close()
        middleware = self._client_handler_resources.get_middleware()
        if middleware:
            middleware.stop()