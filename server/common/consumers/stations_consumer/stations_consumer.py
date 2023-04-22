import json
import logging

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.dag_node import DAGNode


class StationsConsumer(DAGNode):
    def __init__(self, rabbit_hostname: str, data_exchange: str, exchange_type: str, stations_queue_name: str,
                 duplicated_stations_departures_exchange_name: str, duplicated_stations_departures_exchange_type: str,
                 montreal_stations_filter_routing_key: str):
        super().__init__()
        self._rabbit_connection = RabbitBlockingConnection(rabbit_hostname=rabbit_hostname)
        self._stations_queue = RabbitQueue(rabbit_connection=self._rabbit_connection,
                                           queue_name=stations_queue_name,
                                           bind_exchange=data_exchange,
                                           bind_exchange_type=exchange_type,
                                           routing_key=stations_queue_name)
        self._duplicated_stations_departures_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
            exchange_name=duplicated_stations_departures_exchange_name,
            exchange_type=duplicated_stations_departures_exchange_type,
        )

        self._montreal_stations_filter_exchange = RabbitExchange(
            rabbit_connection=self._rabbit_connection,
        )

        self._montreal_stations_filter_routing_key = montreal_stations_filter_routing_key
        self._montreal_fields = ['code', 'city', 'name', 'latitude', 'longitude']
        self._duplicated_stations_fields = ['code', 'city', 'name']

    def run(self):
        try:
            self._stations_queue.consume(lambda *args: self.__on_message_callback(*args))
        except BaseException as e:
            if self.closed:
                logging.info('action: shutdown | status: successful')

    def __on_message_callback(self, ch, method, properties, body):
        obj_message = json.loads(body)
        try:
            if obj_message['payload'] == 'EOF':
                self._duplicated_stations_departures_exchange.publish(message=body, routing_key='')
                self._montreal_stations_filter_exchange.publish(message=body,
                                                                routing_key=self._montreal_stations_filter_routing_key)
            else:
                montreal_filter_payload = self.select_dictionary_fields(obj_message['payload'], self._montreal_fields)
                montreal_filter_message = json.dumps({'type': obj_message['type'], 'payload': montreal_filter_payload})
                duplicated_stations_payload = self.select_dictionary_fields(obj_message['payload'],
                                                                            self._duplicated_stations_fields)
                duplicated_stations_message = json.dumps(
                    {'type': obj_message['type'], 'payload': duplicated_stations_payload})
                self._montreal_stations_filter_exchange.publish(montreal_filter_message,
                                                                self._montreal_stations_filter_routing_key)
                self._duplicated_stations_departures_exchange.publish(duplicated_stations_message)
        except BaseException as e:
            logging.error(f'ERROR: {e}')

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
