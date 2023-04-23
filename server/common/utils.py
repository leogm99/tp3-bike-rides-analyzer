import json
from typing import Any, Dict, List, Union, Callable


def select_message_fields(fields: List[str]) -> Callable:
    def wrapper(func: Callable) -> Callable:
        def inner(self, *args, **kwargs) -> Union[Dict[str, Any], str]:
            obj = args[0]
            if isinstance(obj, str):
                return func(self, obj)
            filtered_obj = {field: obj[field] for field in fields}
            return func(self, filtered_obj)

        return inner

    return wrapper


def message_from_payload(message_type: str) -> Callable:
    def wrapper(func: Callable) -> Callable:
        def inner(self, *args, **kwargs) -> Dict[str, Any]:
            payload = args[0]
            return func(self, json.dumps({'type': message_type, 'payload': payload}))

        return inner

    return wrapper
