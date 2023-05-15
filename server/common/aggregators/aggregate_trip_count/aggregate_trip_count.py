import logging

from common.aggregators.aggregate_trip_count.aggregate_trip_count_middleware import AggregateTripCountMiddleware
from common.aggregators.count_aggregator.count_aggregator import CountAggregator
from typing import Tuple

from common_utils.protocol.payload import Payload
from common_utils.protocol.message import Message, NULL_TYPE
from common_utils.protocol.protocol import Protocol


class AggregateTripCount(CountAggregator):
    def __init__(self,
                 aggregate_keys: Tuple[str, ...],
                 consumers: int = 1,
                 middleware: AggregateTripCountMiddleware = None):
        super().__init__(aggregate_keys)
        self._middleware = middleware
        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_trip_count(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        for obj in message.payload:
            try:
                year = int(obj.data['yearid'])
            except ValueError as e:
                logging.error(f'action: on-message-callback | data-error: {e}')
                raise e from e
            if year == 2016:
                self.aggregate(payload=obj, year_2016=1, year_2017=0)
            elif year == 2017:
                self.aggregate(payload=obj, year_2016=0, year_2017=1)

    def on_producer_finished(self, message, delivery_tag):
        for k, v in self._aggregate_table.items():
            payload = Payload(data={'station': k, 'year_2016': v['year_2016'], 'year_2017': v['year_2017']})
            msg = Message(message_type=NULL_TYPE, payload=payload)
            raw_msg = Protocol.serialize_message(msg)
            self._middleware.send_filter_message(raw_msg)
        eof = Message.build_eof_message()
        raw_eof = Protocol.serialize_message(eof)
        for _ in range(self._consumers):
            self._middleware.send_filter_message(raw_eof)
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(AggregateTripCount, self).close()
            self._middleware.stop()
