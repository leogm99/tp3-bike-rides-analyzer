import json

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.dag_node import DAGNode
import logging


class WeatherConsumer(DAGNode):
    def __init__(self,
                 rabbit_hostname: str,
                 data_exchange: str,
                 exchange_type: str,
                 weather_queue_name: str,
                 precipitation_filter_routing_key: str):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(
            rabbit_hostname=rabbit_hostname,
        )
        self._weather_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=weather_queue_name,
            bind_exchange=data_exchange,
            bind_exchange_type=exchange_type,
            routing_key=weather_queue_name,
        )

        self._precipitation_filter_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )

        self._queue_name = weather_queue_name
        self._precipitation_filter_fields = ['date', 'prectot']
        self._precipitation_filter_routing_key = precipitation_filter_routing_key

    def run(self):
        self._weather_queue.consume(self.__on_message_callback)

    def __on_message_callback(self, ch, method, properties, body):
        obj_message = json.loads(body)
        if obj_message['payload'] != 'EOF':
            obj_message['payload'] = self.select_dictionary_fields(obj_message['payload'],
                                                                   self._precipitation_filter_fields)
            self._precipitation_filter_exchange.publish(json.dumps(obj_message),
                                                        routing_key=self._precipitation_filter_routing_key)
        else:
            self._precipitation_filter_exchange.publish(body, routing_key=self._precipitation_filter_routing_key)

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
