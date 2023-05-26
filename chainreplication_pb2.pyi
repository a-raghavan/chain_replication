from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AppendEntriesRequest(_message.Message):
    __slots__ = ["client", "command", "key", "seqnum", "value"]
    CLIENT_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    SEQNUM_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    client: str
    command: str
    key: str
    seqnum: int
    value: str
    def __init__(self, seqnum: _Optional[int] = ..., command: _Optional[str] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., client: _Optional[str] = ...) -> None: ...

class AppendEntriesResponse(_message.Message):
    __slots__ = ["success"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class KVPair(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class SyncRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class SyncResponse(_message.Message):
    __slots__ = ["entries", "success"]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[KVPair]
    success: bool
    def __init__(self, success: bool = ..., entries: _Optional[_Iterable[_Union[KVPair, _Mapping]]] = ...) -> None: ...
