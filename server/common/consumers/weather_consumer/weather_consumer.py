import logging

from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.dag_node import DAGNode
from common.utils import select_message_fields_decorator, message_from_payload_decorator
from typing import Dict, Union

DATA_EXCHANGE = 'data'
DATA_EXCHANGE_TYPE = 'direct'
WEATHER_QUEUE_NAME = 'weather'
FILTER_BY_PRECIPITATION_ROUTING_KEY = 'filter_by_precipitation'


class WeatherConsumer(DAGNode):
    filter_by_precipitation_fields = ['date', 'prectot', 'city']

    def __init__(self,
                 rabbit_hostname: str,
                 weather_producers: int = 1,
                 weather_consumers: int = 1):
        super().__init__(rabbit_hostname)
        self._weather_queue = RabbitQueue(
            rabbit_connection=self._rabbit_connection,
            queue_name=WEATHER_QUEUE_NAME,
            bind_exchange=DATA_EXCHANGE,
            bind_exchange_type=DATA_EXCHANGE_TYPE,
            routing_key=WEATHER_QUEUE_NAME,
            producers=weather_producers,
        )

        self._filter_by_precipitation_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )
        self._weather_consumers = weather_consumers

    def run(self):
        self._weather_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message_obj, _delivery_tag):
        payload = message_obj['payload']
        if payload != 'EOF':
            self.__send_message_to_filter_by_precipitation(payload)

    def on_producer_finished(self, _message, delivery_tag):
        logging.info(f'sending eof {self._weather_consumers} times')
        for _ in range(self._weather_consumers):
            self.__send_message_to_filter_by_precipitation('EOF')
        self.close()

    @select_message_fields_decorator(fields=filter_by_precipitation_fields)
    @message_from_payload_decorator(message_type='weather')
    def __send_message_to_filter_by_precipitation(self, message: Union[str, Dict]):
        self.publish(message, self._filter_by_precipitation_exchange,
                     routing_key=FILTER_BY_PRECIPITATION_ROUTING_KEY)
