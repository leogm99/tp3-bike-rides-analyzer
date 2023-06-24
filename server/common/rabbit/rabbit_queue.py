from collections import defaultdict
import logging
from typing import Dict
from common_utils.protocol.protocol import Protocol
from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common_utils.protocol.message import CLIENT_ID, TIMESTAMP
from common_utils.KeyValueStore import KeyValueStore

SNAPSHOT_EOF = 'snapshot_eof'

class RabbitQueue:
    def __init__(self,
                 rabbit_connection: RabbitBlockingConnection,
                 queue_name: str = '',
                 bind_exchange: str = '',
                 bind_exchange_type: str = '',
                 routing_key: str = '',
                 producers: int = 1,
                 auto_delete: bool = False):
        self._producers = producers
        self._snapshot_name = f"{SNAPSHOT_EOF}_{queue_name}"
        self._count_eof: KeyValueStore = KeyValueStore.loads(self._snapshot_name, default_type=defaultdict(list))
        logging.info(f'key value store: {self._count_eof._memtable}')
        self._rabbit_connection = rabbit_connection
        self._queue_name = rabbit_connection.queue_declare(queue_name, auto_delete=auto_delete)
        self._consumer_tag = None
        self._global_flush_timestamp: float = 0.0
        if bind_exchange != '':
            rabbit_connection.exchange_declare(exchange_name=bind_exchange,
                                               exchange_type=bind_exchange_type)
            rabbit_connection.queue_bind(queue_name=self._queue_name,
                                         exchange_name=bind_exchange,
                                         routing_key=routing_key)

    def consume(self, on_message_callback, on_producer_finished, on_flush=lambda *_: None, auto_ack=False):
        def wrap_on_message_callback(ch, method, properties, body):
            message = Protocol.deserialize_message(body)
            delivery_tag = method.delivery_tag
            if message.is_flush():
                logging.info(f'action: consume | message: received flush with timestamp {message.timestamp}')
                on_flush(message, delivery_tag)
                self.ack(delivery_tag)
                return
            timestamp = float(message.timestamp)
            if timestamp < self._global_flush_timestamp:
                self.ack(delivery_tag)
                return
            if message.is_eof():
                client_id = message.client_id
                origin = message.origin
                origin_eofs = self._count_eof[client_id]
                if origin not in origin_eofs:
                    self._count_eof.append(client_id, origin)
                    self._count_eof.dumps(snapshot_name=self._snapshot_name)
                origins_cant = len(self._count_eof[client_id])
                ret_call = None
                if origins_cant == self._producers:
                    ret_call = on_producer_finished(message, delivery_tag)
                self.ack(delivery_tag)
                if ret_call is not None:
                    ret_call()
            else:
                ret_call = on_message_callback(message, delivery_tag)
                # TODO: hacer el ack desde la capa de negocio a través del middleware
                #       esto es en TODOS los nodos
                #       la idea es que los acks de la capa de arriba puedan ser acumulados (buffering)
                #self.ack(delivery_tag)
                if ret_call is not None:
                    ret_call()
                # TODO
        self._consumer_tag = self._rabbit_connection.consume(
            queue_name=self._queue_name,
            on_message_callback=wrap_on_message_callback,
            auto_ack=auto_ack,
        )

    def cancel(self):
        if self._consumer_tag is not None:
            self._rabbit_connection.cancel_consumer(self._consumer_tag)

    def ack(self, delivery_tag):
        self._rabbit_connection.ack(delivery_tag=delivery_tag)

    def get_prefetch_count(self):
        return self._rabbit_connection.get_prefetch_count()

    def set_global_flush_timestamp(self, timestamp):
        self._global_flush_timestamp = float(timestamp)

    def flush(self, timestamp):
        self._global_flush_timestamp = float(timestamp)
        self._count_eof.nuke(self._snapshot_name)
