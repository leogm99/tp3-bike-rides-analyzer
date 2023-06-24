from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue
from common_utils.KeyValueStore import KeyValueStore
from collections import defaultdict

FLUSH_EXCHANGE_NAME = 'flush'
FLUSH_EXCHANGE_TYPE = 'fanout'

class Middleware:
    def __init__(self, hostname: str):
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=hostname,
        )

        self._flush_queue = None
        self.timestamp_store = None

    def start(self):
        self._rabbit_connection.start_consuming()

    @classmethod
    def receive(cls, queue: RabbitQueue, on_message_callback, on_end_message_callback, auto_ack=False):
        queue.consume(on_message_callback=on_message_callback,
                      on_producer_finished=on_end_message_callback,
                      auto_ack=auto_ack)

    @classmethod
    def send(cls, message, exchange: RabbitExchange, routing_key: str = ''):
        exchange.publish(message, routing_key)

    def stop(self):
        self._rabbit_connection.close()


    def consume_flush(self, owner, callback):
        self.timestamp_store = KeyValueStore.loads(f"timestamp_store.json", default_type=defaultdict(float))
        self._flush_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name = f"flush_{owner}",
            bind_exchange=FLUSH_EXCHANGE_NAME,
            bind_exchange_type=FLUSH_EXCHANGE_TYPE
        )
        self._flush_queue.consume(lambda: None, lambda: None, callback)
        return self.timestamp_store['timestamp']
