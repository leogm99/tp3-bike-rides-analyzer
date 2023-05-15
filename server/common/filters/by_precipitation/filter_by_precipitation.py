import logging

from common.filters.by_precipitation.filter_by_precipitation_middleware import FilterByPrecipitationMiddleware
from common.filters.numeric_range.numeric_range import NumericRange
from common_utils.protocol.message import Message, WEATHER
from common_utils.protocol.protocol import Protocol


class FilterByPrecipitation(NumericRange):
    def __init__(self,
                 filter_key: str,
                 low: float,
                 high: float,
                 keep_filter_key: bool = False,
                 middleware: FilterByPrecipitationMiddleware = None):
        super().__init__(filter_key, low, high, keep_filter_key)
        self._middleware = middleware
        # Pub/Sub exchange, everyone will get the EOF message
        self._weather_consumers = 1

    def run(self):
        try:
            self._middleware.receive_weather(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message: Message, _delivery_tag):
        to_send, message_obj = super(FilterByPrecipitation, self).on_message_callback(message, _delivery_tag)
        if to_send:
            raw_message = Protocol.serialize_message(message_obj)
            self._middleware.send_joiner_message(raw_message)

    def on_producer_finished(self, message, delivery_tag):
        eof = Message.build_eof_message(message_type=WEATHER)
        raw_eof = Protocol.serialize_message(eof)
        for _ in range(self._weather_consumers):
            self._middleware.send_joiner_message(raw_eof)
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(FilterByPrecipitation, self).close()
            self._middleware.stop()
