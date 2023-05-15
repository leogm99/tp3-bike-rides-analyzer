import logging

from common.consumers.weather_consumer.weather_consumer_middleware import WeatherConsumerMiddleware
from common.dag_node import DAGNode
from common_utils.protocol.message import Message, WEATHER
from common_utils.protocol.protocol import Protocol


class WeatherConsumer(DAGNode):
    filter_by_precipitation_fields = {'date', 'prectot', 'city'}

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

    def on_message_callback(self, message_obj: Message, _delivery_tag):
        if message_obj.is_eof():
            return
        filter_by_precipitation_message = message_obj.pick_payload_fields(self.filter_by_precipitation_fields)
        self.__send_message_to_filter_by_precipitation(filter_by_precipitation_message)

    def on_producer_finished(self, _message, delivery_tag):
        logging.info('received eof')
        eof = Message.build_eof_message(message_type=WEATHER)
        logging.info(eof)
        for _ in range(self._weather_consumers):
            self.__send_message_to_filter_by_precipitation(eof)
        self._middleware.stop()

    def __send_message_to_filter_by_precipitation(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_to_filter(raw_message)

    def close(self):
        if not self.closed:
            super(WeatherConsumer, self).close()
            self._middleware.stop()
