from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetRequest(_message.Message):
    __slots__ = ["key"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ["success", "value"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    success: str
    value: str
    def __init__(self, success: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ["key", "seqnum", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    SEQNUM_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    seqnum: int
    value: str
    def __init__(self, seqnum: _Optional[int] = ..., key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class PutResultRequest(_message.Message):
    __slots__ = ["seqnum", "success"]
    SEQNUM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    seqnum: int
    success: bool
    def __init__(self, seqnum: _Optional[int] = ..., success: bool = ...) -> None: ...

class PutResultResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...
