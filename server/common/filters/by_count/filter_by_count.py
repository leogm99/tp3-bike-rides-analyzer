import json
import logging

from common.filters.by_count.filter_by_count_middleware import FilterByCountMiddleware
from common.filters.numeric_range.numeric_range import NumericRange


class FilterByCount(NumericRange):
    def __init__(self,
                 filter_key: str,
                 low: float,
                 high: float,
                 keep_filter_key: bool = False,
                 middleware: FilterByCountMiddleware = None):
        super().__init__(filter_key, low, high, keep_filter_key)
        self._middleware = middleware

    def run(self):
        try:
            self._middleware.receive_count_aggregate(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message_obj, delivery_tag):
        if message_obj['payload'] == 'EOF':
            return
        # filter data that has no counts the previous year
        if message_obj['payload']['year_2016'] == 0:
            return
        # trips_2017 > 2*trips_2016 <-> trips_2017/2 > trips_2016
        self.high = float(message_obj['payload']['year_2017']) / 2
        to_send, obj = super(FilterByCount, self).on_message_callback(message_obj, delivery_tag)
        if to_send:
            self._middleware.send_metrics_message(json.dumps({'type': 'count_metric', 'payload': obj['payload']}))

    def on_producer_finished(self, message, delivery_tag):
        self._middleware.send_metrics_message(json.dumps({'type': 'count_metric', 'payload': 'EOF'}))
        self._middleware.stop()
