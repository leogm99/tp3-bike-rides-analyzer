import logging
import queue

from common.loader.metrics_waiter import MetricsWaiter
from common.loader.metrics_waiter_middleware import MetricsWaiterMiddleware
from common.loader.static_data_ack_waiter import StaticDataAckWaiter
from common.loader.static_data_ack_waiter_middleware import StaticDataAckWaiterMiddleware


class ClientHandlerResources:
    def __init__(self,
                 hostname,
                 ack_count,
                 middleware_factory):
        self._middleware_factory = middleware_factory

        self._hostname = hostname

        self._middleware = None
        self._static_data_ack_waiter = None
        self._metrics_waiter = None
        self._ack_count = ack_count

        self._metrics_waiter_queue = queue.Queue()


    def acquire(self, client_id):
        self._middleware = self._middleware_factory()
        self._static_data_ack_waiter = StaticDataAckWaiter(self._ack_count, middleware=StaticDataAckWaiterMiddleware(
            hostname=self._hostname,
            client_id=client_id,
        ))

        self._metrics_waiter = MetricsWaiter(local_queue=self._metrics_waiter_queue, middleware=MetricsWaiterMiddleware(
            hostname=self._hostname,
            client_id=client_id,
        ))

        self._static_data_ack_waiter.start()
        self._metrics_waiter.start()
        return self._static_data_ack_waiter.get_wait_event()

    def get_middleware(self):
        return self._middleware

    def get_metrics_queue(self):
        return self._metrics_waiter_queue

    def release(self):
        logging.info('action: client-handler-resources-release | status: in progress')
        if self._middleware:
            self._middleware.stop()
            logging.debug('action: client-handler-resources-release | status: released middleware')
        if self._static_data_ack_waiter:
            self._static_data_ack_waiter.close()
            self._static_data_ack_waiter.join()
            logging.debug('action: client-handler-resources-release | status: released static data ack waiter')
        if self._metrics_waiter:
            self._metrics_waiter.close()
            self._metrics_waiter.join()
            logging.debug('action: client-handler-resources-release | status: released metrics waiter')

        self._static_data_ack_waiter = None
        self._metrics_waiter= None
        self._middleware = None
