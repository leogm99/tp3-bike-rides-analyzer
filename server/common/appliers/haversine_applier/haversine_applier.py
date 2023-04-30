from common.appliers.applier import Applier

class HaversineApplier(Applier):
    def __init__(self, rabbit_hostname: str):
        super().__init__(rabbit_hostname)

    def apply(self, message):
        pass
    pass