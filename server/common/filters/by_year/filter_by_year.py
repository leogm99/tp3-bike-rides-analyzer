import logging

from common.filters.by_year.filter_by_year_middleware import FilterByYearMiddleware
from common.filters.numeric_range.numeric_range import NumericRange
from common_utils.protocol.message import Message, TRIPS, CLIENT_ID
from common_utils.protocol.protocol import Protocol


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
        if message.is_eof():
            return
        to_send, message_obj = super(FilterByYear, self).on_message_callback(message, _delivery_tag)
        if to_send:
            self.__send_joiner_message(message_obj)

    def __send_joiner_message(self, message: Message):
        raw_message = Protocol.serialize_message(message)
        self._middleware.send_joiner_message(raw_message)

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info('action: on-producer-finished | received EOS')
        client_id = message.payload.data[CLIENT_ID]
        eof = Message.build_eof_message(message_type=TRIPS, client_id=client_id)
        for _ in range(self._consumers):
            self.__send_joiner_message(eof)
        # self._middleware.stop()

    def close(self):
        if not self.closed:
            super(FilterByYear, self).close()
            self._middleware.stop()
