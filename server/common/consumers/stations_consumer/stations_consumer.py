import json
import logging

from common.rabbit.rabbit_blocking_connection import RabbitBlockingConnection
from common.rabbit.rabbit_queue import RabbitQueue
from common.rabbit.rabbit_exchange import RabbitExchange
from common.dag_node import DAGNode
from common.utils import select_message_fields, message_from_payload


class StationsConsumer(DAGNode):
    montreal_fields = ['code', 'city', 'name', 'latitude', 'longitude']
    duplicated_stations_fields = ['code', 'city', 'name']

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

    def run(self):
        try:
            self._stations_queue.consume(self.on_message_callback)
        except BaseException as e:
            if self.closed:
                logging.info('action: shutdown | status: successful')
            else:
                raise e

    def on_message_callback(self, body):
        obj_message = json.loads(body)
        payload = obj_message['payload']
        self.__send_message_to_montreal_stations_filter(payload)
        self.__send_message_to_duplicated_stations_joiner(payload)

    @select_message_fields(fields=montreal_fields)
    @message_from_payload(message_type='stations')
    def __send_message_to_montreal_stations_filter(self, message: str):
        self.publish(message, self._montreal_stations_filter_exchange,
                     self._montreal_stations_filter_routing_key)

    @select_message_fields(fields=duplicated_stations_fields)
    @message_from_payload(message_type='stations')
    def __send_message_to_duplicated_stations_joiner(self, message: str):
        self.publish(message, self._duplicated_stations_departures_exchange)

    def close(self):
        if not self.closed:
            self.closed = True
            self._rabbit_connection.close()
