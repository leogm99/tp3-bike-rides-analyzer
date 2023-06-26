import logging

from common.aggregators.aggregator import Aggregator
from common.aggregators.aggregate_trip_count.aggregate_trip_count_middleware import AggregateTripCountMiddleware
from common.aggregators.count_aggregator.count_aggregator import CountAggregator
from typing import Tuple

from common_utils.protocol.payload import Payload
from common_utils.protocol.message import Message, NULL_TYPE, CLIENT_ID, FLUSH
from common_utils.protocol.protocol import Protocol
from common_utils.KeyValueStore import KeyValueStore

ORIGIN = 'aggregate_trip_count'


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
            self._middleware.consume_flush(f"{FLUSH}_{ORIGIN}_{self._middleware._node_id}", self.on_flush)
            self._middleware.receive_trip_count(self.on_message_callback, self.on_producer_finished)
            self._middleware.start()
        except BaseException as e:
            if not self.closed:
                raise e from e
            logging.info('action: run | status: success')

    def on_message_callback(self, message, delivery_tag):
        client_id = message.client_id
        message_id = message.message_id
        if Aggregator.was_message_processed(self._aggregate_table, message_id, client_id):
            logging.info('action: on_message_callback | message: message {message_id} from client {client_id} was already processed, ignoring')
            self._middleware.ack_message(delivery_tag)
            return
        for obj in message.payload:
            try:
                year = int(obj.data['yearid'])
            except ValueError as e:
                logging.error(f'action: on-message-callback | data-error: {e}')
                raise e from e
            if year == 2016:
                self.aggregate(payload=obj, client_id=client_id, year_2016=1, year_2017=0)
            elif year == 2017:
                self.aggregate(payload=obj, client_id=client_id, year_2016=0, year_2017=1)
        Aggregator.register_message_processed(self._aggregate_table, message_id, client_id)
        if self._middleware.save_delivery_tag(delivery_tag):
            # atomic
            self._aggregate_table.dumps(f"aggregate_table.json")
            self._middleware.ack_all()

    def on_producer_finished(self, message: Message, delivery_tag):
        logging.info(f'FINISHED WITH CLIENT ID: {message.client_id}')
        # first we dump the table
        self._aggregate_table.dumps(f"aggregate_table.json")

        client_id = message.client_id
        timestamp = message.timestamp
        if client_id in self._aggregate_table:
            client_results: KeyValueStore = self._aggregate_table[client_id]['data']
            msg_id = 0
            for k, v in client_results.items():
                payload = Payload(data={'station': k, 'year_2016': v['year_2016'], 'year_2017': v['year_2017']})
                msg = Message(message_type=NULL_TYPE, 
                            origin=f"{ORIGIN}_{self._middleware._node_id}",
                            client_id=message.client_id,
                            message_id=msg_id,
                            timestamp=timestamp,
                            payload=payload)
                routing_key = msg_id % self._consumers
                raw_msg = Protocol.serialize_message(msg)
                self._middleware.send_filter_message(raw_msg, routing_key)
                msg_id += 1
        else:
            logging.info(f'NO DATA FOR CLIENT ID: {client_id} |: {self._aggregate_table}')
        eof = Message.build_eof_message(client_id=client_id, timestamp=timestamp, origin=f"{ORIGIN}_{self._middleware._node_id}")
        raw_eof = Protocol.serialize_message(eof)
        for i in range(self._consumers):
            self._middleware.send_filter_message(raw_eof, i)

        # after sending everything, we ack all messages
        self._middleware.ack_all()

        # NOW we can erase this client data (we no longer need it!!)
        # as we can only receive eofs for this client id
        # if another eof arrives (duplicated), then now data will be sent for that client
        # as it was previously sent
        if client_id in self._aggregate_table:
            self._aggregate_table.delete(client_id)
        # on the next flush, the json file will be truncated
        
    def on_flush(self, message: Message, _delivery_tag):
        self._middleware.flush(message.timestamp)
        self._aggregate_table.nuke(f"{ORIGIN}_{self._middleware._node_id}")

    def close(self):
        if not self.closed:
            super(AggregateTripCount, self).close()
            self._middleware.stop()
