import json
import logging
from typing import Tuple

from common.joiners.joiner import Joiner
from common.rabbit.rabbit_exchange import RabbitExchange
from common.rabbit.rabbit_queue import RabbitQueue

STATIONS_EXCHANGE = 'join_by_year_city_station_id_stations'
STATIONS_EXCHANGE_TYPE = 'fanout'
QUEUE_NAME = 'joiner_by_year_city_station_id'
STATIC_DATA_ACK_ROUTING_KEY = 'static_data_ack'
AGGREGATE_TRIP_COUNT_ROUTING_KEY = lambda n: f'aggregate_trip_count_{n}'


class JoinByYearCityStationId(Joiner):
    def __init__(self,
                 index_key: Tuple[str, ...],
                 rabbit_hostname: str,
                 stations_producers: int = 1,
                 trips_producers: int = 1,
                 consumers: int = 1):
        super().__init__(index_key, rabbit_hostname)
        self._stations_input_queue = RabbitQueue(
            self._rabbit_connection,
            bind_exchange=STATIONS_EXCHANGE,
            bind_exchange_type=STATIONS_EXCHANGE_TYPE,
            producers=stations_producers,
        )
        self._trips_input_queue = RabbitQueue(
            self._rabbit_connection,
            queue_name=QUEUE_NAME,
            producers=trips_producers,
        )
        self._output_exchange = RabbitExchange(
            self._rabbit_connection
        )
        self._consumers = consumers

    def run(self):
        self._stations_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._trips_input_queue.consume(self.on_message_callback, self.on_producer_finished)
        self._rabbit_connection.start_consuming()

    def on_message_callback(self, message_obj, delivery_tag):
        payload = message_obj['payload']
        if message_obj['type'] == 'stations':
            if message_obj['payload'] != 'EOF':
                self.insert_into_side_table(payload)
            return
        if payload == 'EOF':
            return
        join_data = []
        for obj in payload:
            obj['code'] = obj.pop('start_station_code')
            data = self.join(obj)
            if data is not None:
                del data['city']
                del data['code']
                join_data.append(data)
        if join_data:
            hashes = self.hash_message(message=join_data, hashing_key='name', hash_modulo=self._consumers)
            for routing_key_suffix, obj in hashes.items():
                self._output_exchange.publish(json.dumps({'payload': obj}),
                                              routing_key=AGGREGATE_TRIP_COUNT_ROUTING_KEY(routing_key_suffix))

    def on_producer_finished(self, message, delivery_tag):
        if message['type'] == 'stations':
            self._stations_input_queue.cancel()
            self._output_exchange.publish(json.dumps({'type': 'notify', 'payload': 'ack'}),
                                          routing_key=STATIC_DATA_ACK_ROUTING_KEY)
            logging.info(f'action: on-producer-finished | len-keys: {len(self._side_table.keys())}')
        if message['type'] == 'trips':
            logging.info('received EOS')
            for i in range(self._consumers):
                self._output_exchange.publish(json.dumps({'payload': 'EOF'}),
                                              routing_key=AGGREGATE_TRIP_COUNT_ROUTING_KEY(i))
            self.close()
