import logging
from typing import Tuple

from common.aggregators.aggregate_trip_distance.aggregate_trip_distance_middleware import \
    AggregateTripDistanceMiddleware
from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator

from common_utils.protocol.payload import Payload
from common_utils.protocol.message import Message, DISTANCE_METRIC, CLIENT_ID
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore

ORIGIN_PREFIX = 'aggregate_trip_distance'

class AggregateTripDistance(RollingAverageAggregator):
    def __init__(self,
                 aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 consumers: int = 1,
                 middleware: AggregateTripDistanceMiddleware = None):
        super().__init__(aggregate_keys, average_key)
        self._middleware = middleware
        self._consumers = consumers

    def run(self):
        try:
            self._middleware.receive_trips_distances(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e

    def on_message_callback(self, message, delivery_tag):
        client_id = message.client_id
        for obj in message.payload:
            self.aggregate(payload=obj, client_id=client_id)

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info(f'FINISHED WITH CLIENT ID: {message.client_id}')
        client_id = message.client_id
        if client_id in self._aggregate_table:
            client_results: KeyValueStore = self._aggregate_table[client_id]
            msg_id = 0
            for k, v in client_results.items():
                payload = Payload(data={'station': k, 'distance': v.current_average})
                msg = Message(message_type=DISTANCE_METRIC, 
                            message_id=msg_id,
                            origin=f'{ORIGIN_PREFIX}_{self._middleware._node_id}',
                            client_id=message.client_id,
                            payload=payload)
                routing_key = msg_id % self._consumers
                raw_msg = Protocol.serialize_message(msg)
                self._middleware.send_filter_message(raw_msg, routing_key)
                msg_id += 1
        else:
            logging.info(f'NO DATA FOR CLIENT ID: {client_id} |: {self._aggregate_table}')
        eof = Message.build_eof_message(message_type=DISTANCE_METRIC, 
                                        origin=f'{ORIGIN_PREFIX}_{self._middleware._node_id}',
                                        client_id=client_id)
        raw_eof = Protocol.serialize_message(eof)
        for i in range(self._consumers):
            self._middleware.send_filter_message(raw_eof, i)
        

    def close(self):
        if not self.closed:
            super(AggregateTripDistance, self).close()
            self._middleware.stop()


