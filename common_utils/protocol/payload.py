from dataclasses import dataclass
from typing import Union, Dict, Any, Set

EOF = 'EOF'
ACK = 'ACK'
ID = 'ID'


@dataclass
class Payload:
    data: Union[str, Dict[str, Any]]

    def pick_payload_fields(self, fields_set: Set):
        return Payload(data={k: v for k, v in self.data.items() if k in fields_set})

    def is_eof(self):
        return self.data == EOF

    def is_ack(self):
        return self.data == ACK
