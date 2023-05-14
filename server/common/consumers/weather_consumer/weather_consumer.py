import logging

from common.consumers.weather_consumer.weather_consumer_middleware import WeatherConsumerMiddleware
from common.dag_node import DAGNode
from common.utils import select_message_fields_decorator, message_from_payload_decorator
from typing import Dict, Union


class WeatherConsumer(DAGNode):
    filter_by_precipitation_fields = ['date', 'prectot', 'city']

    def __init__(self,
                 weather_consumers: int = 1,
                 middleware: WeatherConsumerMiddleware = None):
        super().__init__()
        self._middleware = middleware
        self._weather_consumers = weather_consumers

    def run(self):
        try:
            self._middleware.receive_weather(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e

    def on_message_callback(self, message_obj, _delivery_tag):
        payload = message_obj['payload']
        if payload != 'EOF':
            self.__send_message_to_filter_by_precipitation(payload)

    def on_producer_finished(self, _message, delivery_tag):
        for _ in range(self._weather_consumers):
            self.__send_message_to_filter_by_precipitation('EOF')
        self._middleware.stop()

    @select_message_fields_decorator(fields=filter_by_precipitation_fields)
    @message_from_payload_decorator(message_type='weather')
    def __send_message_to_filter_by_precipitation(self, message: Union[str, Dict]):
        self._middleware.send_to_filter(message)

    def close(self):
        if not self.closed:
            super(WeatherConsumer, self).close()
            self._middleware.stop()
