
class KeyValueStore:
    def __init__(self, dict_type={}):
        self._memtable = dict_type

    def get(self, key, default=None):
        return self._memtable.get(key, default)

    def put(self, key, value):
        self._memtable[key] = value
    
    def update(self, key, **kwargs):
        self._memtable[key].update(**kwargs)
    
    def _nuke(self):
        self._memtable = {}
