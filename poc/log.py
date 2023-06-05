import json
import os
import struct
import tempfile
from collections import deque
from abc import ABC, abstractmethod

ENCODING = 'utf8'
SET_VALUE_OPCODE = 1
COMMIT_OPCODE = 2
SNAPSHOT_OPCODE = 3

class LogEntry(ABC):
    def __init__(self) -> None:
        super().__init__()
        pass

    @abstractmethod
    def serialize(self):
        pass

    def is_snapshot(self):
        return False

    @staticmethod
    def deserialize_from_opcode(raw_bytes: bytes):
        opcode = raw_bytes[0]
        LogEntryClass = None
        if opcode == SET_VALUE_OPCODE:
            LogEntryClass = SetValue
        elif opcode == COMMIT_OPCODE:
            LogEntryClass = Commit
        elif opcode == SNAPSHOT_OPCODE:
            LogEntryClass = Snapshot
        return LogEntryClass.deserialize(raw_bytes[1:len(raw_bytes)-1].decode(ENCODING))

class SetValue(LogEntry):
    def __init__(self, key, value) -> None:
        super().__init__()
        self._key = key
        self._value = value

    def serialize(self):
        payload = f"{json.dumps({self._key: self._value})}\n".encode(ENCODING)
        buffer = bytearray()
        buffer.append(SET_VALUE_OPCODE)
        buffer.extend(payload)
        return buffer

    @classmethod 
    def deserialize(cls, raw_entry: bytes):
        entry = json.loads(raw_entry)
        k, v = entry.popitem()
        return SetValue(key=k, value=v)
    
    def into_pair(self):
        return self._key, self._value
    
    def __str__(self) -> str:
        k, v = self.into_pair()
        return f'k: {k}, v: {v}'
    

class Commit(LogEntry):
    def __init__(self, msg_id) -> None:
        super().__init__()
        self._msg_id = msg_id
    
    def serialize(self):
        return struct.pack('!bIc', f'{COMMIT_OPCODE}{self._msg_id}\n')
    
    @classmethod
    def deserialize(cls, raw_bytes: bytes):
        pass


class Snapshot(LogEntry):
    def __init__(self) -> None:
        super().__init__()

    def serialize(self):
        return struct.pack('b', SNAPSHOT_OPCODE)
    
    @classmethod
    def deserialize(cls, raw_bytes: bytes):
        pass

    def is_snapshot(self):
        return True 
    
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

class Log:
    LOG = './log'
    def __init__(self) -> None:
        self._membuf = deque()
        self._log = open(self.LOG, mode='a+b')

    def append(self, entry: LogEntry):
        self._membuf.append(entry.serialize())
        if entry.is_snapshot():
            self._force_flush()

    def yield_entries(self):
        self._log.seek(os.SEEK_SET)
        for row in self._log:
            yield LogEntry.deserialize_from_opcode(row)

    def _force_flush(self):
        self._log.writelines(self._membuf)
        self._log.flush()
        self._membuf.clear()

    def _prune(self):
        pass

    def close(self):
        self._force_flush()
        self._log.close()


class KeyValueStore:
    SNAPSHOT_FILE = 'kv.snapshot'
    def __init__(self, log: Log) -> None:
        self._memtable = {}
        self._log = log
        self._restore()

    def get(self, key):
        return self._memtable.get(key)

    def put(self, key, value):
        self._log.append(SetValue(key=key, value=value))
        self._memtable[key] = value
    
    def _nuke(self):
        self._memtable = {}

    def _restore(self):
        try:
            with open(self.SNAPSHOT_FILE, 'r') as sn:
                self._memtable = json.loads(sn.readlines()[0])
        except FileNotFoundError:
            pass
        # TODO: refactor (para distintos tipos de LogEntry)
        for entry in self._log.yield_entries():
            if hasattr(entry, 'into_pair'):
                k, v = entry.into_pair()
                self._memtable[k] = v
        self.take_snapshot()

    def take_snapshot(self):
        with RenamedTemporaryFile(self.SNAPSHOT_FILE, delete=False) as f:
            f.write(json.dumps(self._memtable).encode(ENCODING))

'''
    snapshot
    ---
    k: v
    k: v
    k: v

    commit 2001

    k: v
    k: v
    k: v

    commit 2002

    ...

    commit 4000
    snapshot
'''

# snapshot -> aplico todo el log hasta el ultimo snapshot -> tomo un snapshot


if __name__ == '__main__':
    log = Log()
    kv_store = KeyValueStore(log=log)
    print(kv_store._memtable)
    '''
    log = Log()
    kv_store = KeyValueStore(log=log)
    msg_1 = {'a': 1, 'b': 2}
    msg_2 = {'b': 3, 'c': 3}
    msg_3 = {'d': 40, 'v': 5, 'x': 1, 'y': 7}
    msg_4 = {'a': 10, 'r': 912}
    msg_5 = {'a': 30, 't': 12, 'w': 32}
    snap = Snapshot()
    msgs = [msg_1, msg_2, msg_3, msg_4, msg_5]

    for msg in msgs:
        for k,v in msg.items():
            kv_store.put(k, v)
    log.append(snap)
    raise 
    kv_store.take_snapshot()
    '''