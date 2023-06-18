import json
import os
import tempfile


'''
{'client_id': ['origin', '...'] }
'''

class KeyValueStore:
    def __init__(self, dict_type={}):
        self._memtable = dict_type

    def get(self, key, default=None):
        return self._memtable.get(key, default)

    def put(self, key, value):
        self._memtable[key] = value
    
    def update(self, key, **kwargs):
        self._memtable[key].update(**kwargs)

    def append(self, key, value):
        self._memtable[key].append(value)
    
    def items(self):
        return self._memtable.items()
    
    def getAll(self):
        return self._memtable

    def _nuke(self):
        self._memtable = {}

    def __getitem__(self, key):
        return self._memtable[key]

    def __setitem__(self, key, value):
        self._memtable[key] = value

    def dumps(self, snapshot_name):
        with RenamedTemporaryFile(snapshot_name, delete=False) as f:
            f.write(json.dumps(self._memtable).encode())
        
    @staticmethod
    def loads(snapshot_name, default_type):
        kv_store = KeyValueStore(dict_type=default_type)
        try:
            with open(snapshot_name, 'r') as f:
                kv_store._memtable = json.loads(f.read())
            return kv_store
        except BaseException as e:
            return kv_store 


# https://stackoverflow.com/a/12007885
class RenamedTemporaryFile(object):
    """
    A temporary file object which will be renamed to the specified
    path on exit.
    """
    def __init__(self, final_path, **kwargs):
        tmpfile_dir = kwargs.pop('dir', None)

        # Put temporary file in the same directory as the location for the
        # final file so that an atomic move into place can occur.

        if tmpfile_dir is None:
            tmpfile_dir = os.path.dirname(final_path)

        self.tmpfile = tempfile.NamedTemporaryFile(dir=tmpfile_dir, **kwargs)
        self.final_path = final_path

    def __getattr__(self, attr):
        """
        Delegate attribute access to the underlying temporary file object.
        """
        return getattr(self.tmpfile, attr)

    def __enter__(self):
        self.tmpfile.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.tmpfile.delete = False
            result = self.tmpfile.__exit__(exc_type, exc_val, exc_tb)
            os.rename(self.tmpfile.name, self.final_path)
        else:
            result = self.tmpfile.__exit__(exc_type, exc_val, exc_tb)
        return result
