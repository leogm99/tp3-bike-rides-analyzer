import json
import logging

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.dag_node import DAGNode
from common.utils import select_message_fields, message_from_payload


class WeatherConsumer(DAGNode):
    precipitation_filter_fields = ['date', 'prectot']

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

        self._precipitation_filter_routing_key = precipitation_filter_routing_key

    def run(self):
        self._weather_queue.consume(self.on_message_callback)

    def on_message_callback(self, body):
        obj_message = json.loads(body)
        payload = obj_message['payload']
        self.__send_message_to_precipitation_filter(payload)

    @select_message_fields(fields=precipitation_filter_fields)
    @message_from_payload(message_type='weather')
    def __send_message_to_precipitation_filter(self, message: str):
        self.publish(message, self._precipitation_filter_exchange,
                     routing_key=self._precipitation_filter_routing_key)

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
