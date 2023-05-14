import json
import logging

from common.filters.by_year.filter_by_year_middleware import FilterByYearMiddleware
from common.filters.numeric_range.numeric_range import NumericRange


class FilterByYear(NumericRange):
    def __init__(self,
                 filter_key: str,
                 low: float,
                 high: float,
                 keep_filter_key: bool = False,
                 consumers: int = 1,
                 middleware: FilterByYearMiddleware = None):
        super().__init__(filter_key, low, high, keep_filter_key)
        self._middleware = middleware
        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, _delivery_tag):
        if message['payload'] == 'EOF':
            return
        to_send, message_obj = super(FilterByYear, self).on_message_callback(message, _delivery_tag)
        if to_send:
            self._middleware.send_joiner_message(json.dumps(message_obj))

    def on_producer_finished(self, message, delivery_tag):
        logging.info('action: on-producer-finished | received EOS')
        for _ in range(self._consumers):
            self._middleware.send_joiner_message(json.dumps({'type': 'trips', 'payload': 'EOF'}))
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(FilterByYear, self).close()
            self._middleware.stop()
