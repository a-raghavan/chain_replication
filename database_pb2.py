# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: database.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0e\x64\x61tabase.proto\x12\x08\x64\x61tabase\"\x19\n\nGetRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\"\x1c\n\x0bGetResponse\x12\r\n\x05value\x18\x02 \x01(\t\"8\n\nPutRequest\x12\x0e\n\x06seqnum\x18\x01 \x01(\x03\x12\x0b\n\x03key\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\t\"\r\n\x0bPutResponse\"3\n\x10PutResultRequest\x12\x0e\n\x06seqnum\x18\x01 \x01(\x03\x12\x0f\n\x07success\x18\x02 \x01(\x08\"\x13\n\x11PutResultResponse2\xbe\x01\n\x08\x44\x61tabase\x12\x34\n\x03Get\x12\x14.database.GetRequest\x1a\x15.database.GetResponse\"\x00\x12\x34\n\x03Put\x12\x14.database.PutRequest\x1a\x15.database.PutResponse\"\x00\x12\x46\n\tPutResult\x12\x1a.database.PutResultRequest\x1a\x1b.database.PutResultResponse\"\x00\x42\x30\n\x18io.grpc.examples.leveldbB\x0cLevelDBProtoP\x01\xa2\x02\x03HLWb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'database_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\030io.grpc.examples.leveldbB\014LevelDBProtoP\001\242\002\003HLW'
  _GETREQUEST._serialized_start=28
  _GETREQUEST._serialized_end=53
  _GETRESPONSE._serialized_start=55
  _GETRESPONSE._serialized_end=83
  _PUTREQUEST._serialized_start=85
  _PUTREQUEST._serialized_end=141
  _PUTRESPONSE._serialized_start=143
  _PUTRESPONSE._serialized_end=156
  _PUTRESULTREQUEST._serialized_start=158
  _PUTRESULTREQUEST._serialized_end=209
  _PUTRESULTRESPONSE._serialized_start=211
  _PUTRESULTRESPONSE._serialized_end=230
  _DATABASE._serialized_start=233
  _DATABASE._serialized_end=423
# @@protoc_insertion_point(module_scope)
