import logging

from common.aggregators.aggregate_trip_duration.aggregate_trip_duration_middleware import \
    AggregateTripDurationMiddleware
from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from typing import Tuple
from common_utils.protocol.payload import Payload
from common_utils.protocol.message import Message, DURATION_METRIC, CLIENT_ID
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore


class AggregateTripDuration(RollingAverageAggregator):
    def __init__(self, aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 middleware: AggregateTripDurationMiddleware):
        super().__init__(aggregate_keys, average_key)
        self._middleware = middleware

    def run(self):
        try:
            self._middleware.receive_trip_duration(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        for obj in message.payload:
            obj.data['duration_sec'] = max(float(obj.data['duration_sec']), 0.)
            super(AggregateTripDuration, self).aggregate(payload=obj)

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info('action: on-producer-finished | END OF STREAM RECEIVED')
        client_id = message.payload.data[CLIENT_ID]
        client_results: KeyValueStore = self._aggregate_table[client_id]
        for k, v in client_results.items():
            payload = Payload(data={CLIENT_ID: client_id, 'date': k, 'duration_sec': v.current_average})
            msg = Message(message_type=DURATION_METRIC, payload=payload)
            raw_msg = Protocol.serialize_message(msg)
            self._middleware.send_metrics_message(raw_msg)
        eof = Message.build_eof_message(message_type=DURATION_METRIC, client_id=client_id)
        raw_eof = Protocol.serialize_message(eof)
        self._middleware.send_metrics_message(raw_eof)
        self._middleware.stop()

    def close(self):
        if not self.closed:
            super(AggregateTripDuration, self).close()
            self._middleware.stop()
