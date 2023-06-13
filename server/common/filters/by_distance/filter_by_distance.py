import logging

from common.filters.by_distance.filter_by_distance_middleware import FilterByDistanceMiddleware
from common.filters.numeric_range.numeric_range import NumericRange
from common_utils.protocol.message import Message, DISTANCE_METRIC, CLIENT_ID
from common_utils.protocol.protocol import Protocol


class FilterByDistance(NumericRange):
    def __init__(self, filter_key: str,
                 low: float,
                 high: float,
                 keep_filter_key: bool = False,
                 middleware: FilterByDistanceMiddleware = None):
        super().__init__(filter_key, low, high, keep_filter_key)
        self._middleware = middleware

    def run(self):
        try:
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, _delivery_tag):
        if message.is_eof():
            return
        to_send, message_obj = super(FilterByDistance, self).on_message_callback(message, _delivery_tag)
        if to_send:
            message_obj.message_type = DISTANCE_METRIC
            raw_msg = Protocol.serialize_message(message_obj)
            self._middleware.send_metrics_message(raw_msg)

    def on_producer_finished(self, message: Message, delivery_tag):
        client_id = message.payload.data[CLIENT_ID]
        eof = Message.build_eof_message(message_type=DISTANCE_METRIC, client_id=client_id)
        raw_eof = Protocol.serialize_message(eof)
        self._middleware.send_metrics_message(raw_eof)
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(FilterByDistance, self).close()
            self._middleware.stop()
