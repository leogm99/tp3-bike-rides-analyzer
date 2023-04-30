from typing import Tuple

from common.joiners.joiner import Joiner


class JoinByYearEndStationId(Joiner):
    def __init__(self, index_key: Tuple[str, ...], rabbit_hostname: str):
        super().__init__(index_key, rabbit_hostname)

    def run(self):
        pass

    def on_message_callback(self, message, delivery_tag):
        pass

    def on_producer_finished(self, message, delivery_tag):
        pass
