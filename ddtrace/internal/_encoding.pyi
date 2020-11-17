from typing import Any, Union

class MsgpackEncoder(object):
    content_type: str

    def _decode(self, data: Union[str, bytes]) -> Any: ...
