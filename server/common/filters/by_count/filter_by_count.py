import json
import logging

from common.filters.by_count.filter_by_count_middleware import FilterByCountMiddleware
from common.filters.numeric_range.numeric_range import NumericRange
from common_utils.protocol.message import Message, COUNT_METRIC, CLIENT_ID
from common_utils.protocol.protocol import Protocol

ORIGIN_PREFIX = 'filter_by_count'

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

    def on_message_callback(self, message_obj: Message, delivery_tag):
        if message_obj.is_eof():
            return
        # filter data that has no counts the previous year
        if message_obj.payload.data['year_2016'] == 0:
            return
        # trips_2017 > 2*trips_2016 <-> trips_2017/2 > trips_2016
        self.high = float(message_obj.payload.data['year_2017']) / 2
        to_send, obj = super(FilterByCount, self).on_message_callback(message_obj, delivery_tag)
        if to_send:
            obj.message_type = COUNT_METRIC
            raw_message = Protocol.serialize_message(obj)
            self._middleware.send_metrics_message(raw_message)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.client_id
        eof = Message.build_eof_message(message_type=COUNT_METRIC, client_id=client_id, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
        raw_eof = Protocol.serialize_message(eof)
        self._middleware.send_metrics_message(raw_eof)
        

    def close(self):
        if not self.closed:
            super(FilterByCount, self).close()
            self._middleware.stop()
