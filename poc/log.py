import json
import os
from collections import deque
from typing import Any, Union, List
from abc import ABC, abstractmethod

ENCODING = 'utf8'
SET_VALUE_OPCODE = 1
COMMIT_OPCODE = 2

class LogEntry(ABC):
    def __init__(self, code) -> None:
        super().__init__()
        pass

    @abstractmethod
    def deserialize(string_entry: str):
        pass

    @abstractmethod
    def serialize(self):
        pass

    @staticmethod
    def deserialize_from_opcode(raw_bytes: bytes):
        opcode = raw_bytes[0]
        LogEntryClass = None
        if opcode == SET_VALUE_OPCODE:
            LogEntryClass = SetValue
        elif opcode == COMMIT_OPCODE:
            LogEntryClass = Commit
        return LogEntryClass.deserialize(raw_entry=raw_bytes[1:len(raw_bytes)-1].decode(ENCODING))



class SetValue(LogEntry):
    def __init__(self, key, value) -> None:
        super().__init__(code=SET_VALUE_OPCODE)
        self._key = key
        self._value = value

    def serialize(self):
        payload = f"{json.dumps({k: v})}\n".encode(ENCODING)
        buffer = bytearray()
        buffer.append(SET_VALUE_OPCODE)
        buffer.extend(payload)
        return buffer
    
    def deserialize(raw_entry: bytes):
        entry = json.loads(raw_entry)
        k, v = entry.popitem()
        return SetValue(key=k, value=v)
    
    def into_pair(self):
        return self._key, self._value
    
    def __str__(self) -> str:
        return f'k: {self._key}, v: {self._value}'
    

class Commit(LogEntry):
    def __init__(self) -> None:
        super().__init__(code=COMMIT_OPCODE)
    
    def serialize(self, msg_id):
        return 
    
    
'''

    Entries del log:
    (esto no es asi pero podría ser una forma)
    <msg_id> <k1:v1> ... <kn: vn>
    <msg_id> <k1:v1> ... <kn: vn>

    (actualmente es asi):

    (k: v)
    (k: v)
    (k: v)
       .
       .
       .
    commit <msg_id>

    ...
    Entre commits se encuentran todos los cambios efectuados por el <msg_id>
    Si no aparece un commit podemos definir a ese mensaje como no procesado y descartar esa parte del log (depende si las operaciones son idempotentes)?
    En caso que sean idempot. podemos ignorar que falta un commit y rearmar el estado desde alli.
    Cuando trimmeamos?

'''

class Log:
    LOG = './log'
    def __init__(self, maxsize) -> None:
        self._maxsize = maxsize
        self._membuf = deque(maxlen=self._maxsize)
        self._currcount = 0
        self._log = open(self.LOG, mode='a+b')

    def append(self, entry: LogEntry):
        self._try_flush()
        self._membuf.append(entry.serialize())
        self._currcount += 1

    def yield_entries(self):
        self._log.seek(os.SEEK_SET)
        for row in self._log:
            yield LogEntry.deserialize_from_opcode(row)
            

    def _try_flush(self):
        if self._currcount != self._maxsize:
            return
        self._force_flush()
    
    def _force_flush(self):
        self._log.writelines(self._membuf)
        self._log.flush()
        self._membuf.clear()
        self._currcount = 0

    def close(self):
        self._force_flush()
        self._log.close()


class KeyValueStore:
    SNAPSHOT_FILE_PREFIX = 'snapshot'
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
        # TODO: refactor (para distintos tipos de LogEntry)
        for entry in self._log.yield_entries():
            if hasattr(entry, 'into_pair'):
                k, v = entry.into_pair()
                self._memtable[k] = v

    def take_snapshot(self):
        with open(self.SNAPSHOT_FILE_PREFIX) as f:
            f.write(json.dumps(self._memtable))
            f.flush()
            os.fsync()

if __name__ == '__main__':
    log = Log(maxsize=2)
    kv_store = KeyValueStore(log=log)
    msg_1 = {'a': 1, 'b': 2}
    msg_2 = {'b': 3, 'c': 3}
    msgs = [msg_1, msg_2,]

    for msg in msgs:
        for k,v in msg.items():
            kv_store.put(k, v)

    kv_store._nuke()
    print(kv_store._memtable)
    kv_store._restore()
    print(kv_store._memtable)
