import json
from typing import Any, Dict, List, Union, Callable


def select_message_fields_decorator(fields: List[str]) -> Callable:
    def wrapper(func: Callable) -> Callable:
        def inner(self, *args, **kwargs) -> Union[List[Dict[str, Any]], Dict[str, Any], str]:
            obj = args[0]
            if isinstance(obj, str):
                return func(self, obj)
            elif isinstance(obj, list):
                buffer = []
                for i, o in enumerate(obj):
                    buffer.append({k: v for k, v in o.items() if k in fields})
                return func(self, buffer)
            return func(self, {k: v for k, v in obj.items() if k in fields})

        return inner

    return wrapper


def message_from_payload_decorator(message_type: str) -> Callable:
    def wrapper(func: Callable) -> Callable:
        def inner(self, *args, **kwargs) -> Dict[str, Any]:
            payload = args[0]
            return func(self, json.dumps({'type': message_type, 'payload': payload}))

        return inner

    return wrapper
