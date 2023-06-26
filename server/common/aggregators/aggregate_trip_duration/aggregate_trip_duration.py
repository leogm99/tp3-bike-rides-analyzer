import logging

from common.aggregators.aggregator import Aggregator
from common.aggregators.aggregate_trip_duration.aggregate_trip_duration_middleware import \
    AggregateTripDurationMiddleware
from common.aggregators.rolling_average_aggregator.rolling_average_aggregator import RollingAverageAggregator
from typing import Tuple
from common_utils.protocol.payload import Payload
from common_utils.protocol.message import Message, DURATION_METRIC, CLIENT_ID, FLUSH
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore

ORIGIN = 'aggregate_trip_duration'


class AggregateTripDuration(RollingAverageAggregator):
    def __init__(self, aggregate_keys: Tuple[str, ...],
                 average_key: str,
                 middleware: AggregateTripDurationMiddleware):
        super().__init__(aggregate_keys, average_key)
        self._middleware = middleware

    def run(self):
        try:
            self._middleware.consume_flush(f"{FLUSH}_{ORIGIN}_{self._middleware._node_id}", self.on_flush)
            self._middleware.receive_trip_duration(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        client_id = message.client_id
        message_id = message.message_id
        if Aggregator.was_message_processed(self._aggregate_table, message_id=message_id, client_id=client_id):
            self._middleware.ack_message(delivery_tag)
            return
        for obj in message.payload:
            obj.data['duration_sec'] = max(float(obj.data['duration_sec']), 0.)
            self.aggregate(payload=obj, client_id=client_id)
        Aggregator.register_message_processed(self._aggregate_table, message_id=message_id, client_id=client_id)
        if self._middleware.save_delivery_tag(delivery_tag):
            self._aggregate_table.dumps('aggregate_table.json')
            self._middleware.ack_all()

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info(f'FINISHED WITH CLIENT ID: {message.client_id}')
        self._aggregate_table.dumps('aggregate_table.json')
        client_id = message.client_id
        timestamp = message.timestamp
        if client_id in self._aggregate_table:
            client_results: KeyValueStore = self._aggregate_table[client_id]['data']
            logging.info(f'CLIENT RESULTS: {client_results}')
            msg_id = 0
            for k, v in client_results.items():
                payload = Payload(data={'date': k, 'duration_sec': v.current_average})
                msg = Message(message_type=DURATION_METRIC, 
                            origin=f"f{ORIGIN}_{self._middleware._node_id}",
                            message_id=msg_id,
                            timestamp=timestamp,
                            payload=payload, 
                            client_id=message.client_id)
                raw_msg = Protocol.serialize_message(msg)
                self._middleware.send_metrics_message(raw_msg)
                msg_id += 1
        else:
            logging.info(f'NO DATA FOR CLIENT ID: {client_id} |: {self._aggregate_table}')
        eof = Message.build_eof_message(message_type=DURATION_METRIC, 
                                        timestamp=timestamp,
                                        origin=f"f{ORIGIN}_{self._middleware._node_id}", 
                                        client_id=client_id)
        raw_eof = Protocol.serialize_message(eof)
        self._middleware.send_metrics_message(raw_eof)
        self._middleware.ack_all()
        if client_id in self._aggregate_table:
            self._aggregate_table.delete(client_id)
        
    def on_flush(self, message: Message, _delivery_tag):
        self._middleware.flush(message.timestamp)
        self._aggregate_table.nuke(f"{ORIGIN}_{self._middleware._node_id}")

    def close(self):
        if not self.closed:
            super(AggregateTripDuration, self).close()
            self._middleware.stop()
