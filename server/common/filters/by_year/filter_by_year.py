import logging

from common.filters.by_year.filter_by_year_middleware import FilterByYearMiddleware
from common.filters.numeric_range.numeric_range import NumericRange
from common_utils.protocol.message import Message, TRIPS, CLIENT_ID, FLUSH
from common_utils.protocol.protocol import Protocol

ORIGIN_PREFIX = 'filter_by_year'

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
            self._middleware.consume_flush(f"{FLUSH}_{ORIGIN_PREFIX}_{self._middleware._node_id}", self.on_flush)
            self._middleware.receive_trips(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        if message.is_eof():
            return
        to_send, message_obj = super(FilterByYear, self).on_message_callback(message, delivery_tag)
        if to_send:
            self.__send_joiner_message(message_obj)
        self._middleware.ack_message(delivery_tag)

    def __send_joiner_message(self, message: Message):
        if not message.is_eof():
            routing_key = int(message.message_id) % self._consumers
            raw_msg = Protocol.serialize_message(message)
            self._middleware.send_joiner_message(raw_msg, routing_key)
        else:
            raw_msg = Protocol.serialize_message(message)
            for i in range(self._consumers):
                self._middleware.send_joiner_message(raw_msg, i)

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info('action: on-producer-finished | received EOS')
        client_id = message.client_id
        timestamp = message.timestamp
        eof = Message.build_eof_message(message_type=TRIPS, client_id=client_id, timestamp=timestamp, origin=f"{ORIGIN_PREFIX}_{self._middleware._node_id}")
        self.__send_joiner_message(eof)
        
    def on_flush(self, message: Message, _delivery_tag):
        self._middleware.flush(message.timestamp)

    def close(self):
        if not self.closed:
            super(FilterByYear, self).close()
            self._middleware.stop()
