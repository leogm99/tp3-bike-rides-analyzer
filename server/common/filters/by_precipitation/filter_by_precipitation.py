import json
import logging

from common.filters.by_precipitation.filter_by_precipitation_middleware import FilterByPrecipitationMiddleware
from common.filters.numeric_range.numeric_range import NumericRange


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

    def on_message_callback(self, message, _delivery_tag):
        to_send, message_obj = super(FilterByPrecipitation, self).on_message_callback(message, _delivery_tag)
        if to_send:
            self._middleware.send_joiner_message(json.dumps(message_obj))

    def on_producer_finished(self, message, delivery_tag):
        eof = {'type': 'weather', 'payload': 'EOF'}
        logging.info('sending eof')
        for _ in range(self._weather_consumers):
            self._middleware.send_joiner_message(json.dumps(eof))
        self._middleware.stop()
