from kazoo.client import KazooClient
import leveldb
from collections import deque
import threading
from concurrent import futures
import grpc
import chainreplication_pb2
import chainreplication_pb2_grpc
import database_pb2
import database_pb2_grpc
import time
from enum import Enum

class QueueEntryState(Enum):
    INIT = 0
    SENT = 1
    RECD = 2
    ACKD = 3

class QueueEntry:
    def __init__(self, seqnum, cmd, key, value, client, state) -> None:
        self.seqnum = seqnum
        self.command = cmd
        self.key = key
        self.value = value
        self.client = client
        self.state = state

class ChainReplicator(chainreplication_pb2_grpc.ChainReplicationServicer, database_pb2_grpc.DatabaseServicer):

    def __init__(self, nodeport:str) -> None:
        self.bootstrap = True
        
        self.mynodeport = nodeport

        # set up zookeeper
        self.zk = KazooClient(hosts='127.0.0.1:2181')
        self.zk.start()

        # ensure necessart zk paths
        self.zk.ensure_path("/cluster")
        self.zk.ensure_path("/cluster/tail")
        self.zk.ensure_path("/cluster/head")

        # instantiate levelDB
        self.db = self.setupDB(id)      

        self.crthreadpool = futures.ThreadPoolExecutor(max_workers=1)

        # start chain replication grpc server thread
        self.crthread = threading.Thread(self.__chainReplicationListener)
        
        # Current tail notices when a new node is created in ZK
        # It starts propagating new reqs to the new node. 
        # This is queued up until the synchroniztion is completed

        self.myzknode = self.zk.create(path="/cluster/guid_", value=nodeport.encode(), ephemeral=True, sequence=True)

        peers = self.zk.get_children(path="/cluster")
        if len(peers) == 0:
            self.head = nodeport
            self.tail = nodeport
            self.zk.set("/cluster/head", nodeport.encode())
        else:
            # sync from curr tail
            self.syncWithCurrentTail(max(peers))
            self.tail = nodeport
        
        self.zk.set("/cluster/tail", nodeport.encode())

        # start gRPC database server thread
        self.dbthread = threading.Thread(self.__databaseListener)
        
        self.peer = ""                         # peer is initially none since i'm the tail
        
        self.sentQueue = deque()                 # sent queue contains entries sent to peer not acked by tail

        # needed to ensure new data is committed to disk only after synchronization (i.e. bootstrap) is completed
        self.bootstrap = False
 
        return

    def __del__(self):
        self.zk.stop()
    
    def __chainReplicationListener(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        chainreplication_pb2_grpc.add_ChainReplicationServicer_to_server(self, server)
        server.add_insecure_port('[::]:' + "50051")
        server.start()
        print("Chain replication server started, listening on " + "50051")
        server.wait_for_termination()
    
    def __databaseListener(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        database_pb2_grpc.add_DatabaseServicer_to_server(self, server)
        server.add_insecure_port('[::]:' + "50052")
        server.start()
        print("Database server started, listening on " + "50052")
        server.wait_for_termination()

    def syncWithCurrentTail(self, tailzknode):
        # get current tail
        tailNodePort = self.zk.get("/cluster/"+tailzknode)[0]

        # send sync request from current tail
        with grpc.insecure_channel(tailNodePort) as channel:
            stub = chainreplication_pb2_grpc.ChainReplicationStub(channel)
            response = chainreplication_pb2.SyncResponse(success=False, bucket=[])
            while response.success == False:
                try:
                    response = stub.Sync(chainreplication_pb2.SyncRequest())
                except Exception as e:
                    time.sleep(0.5)
                    continue
        
        # commit response entries to levelDB
        for e in response.entries:
            self.db.Put(bytearray(e.key, encoding="utf8"), bytearray(e.value, encoding="utf8"))

        return

    def Sync(self, request, context):
        # get all data from levelDB
        itr = self.db.RangeIter()
        data = []
        for key, value in itr:
            data.append(chainreplication_pb2.KVPair(key=key, value=value))
        # send it to new tail
        return chainreplication_pb2.SyncResponse(success=True, entries=data)
    
    def __appendEntries(self, request:chainreplication_pb2.AppendEntriesRequest):
        if self.peer == "":
            return

        with grpc.insecure_channel(self.peer) as channel:
            stub = chainreplication_pb2_grpc.ChainReplicationStub(channel)
            response = chainreplication_pb2.AppendEntriesResponse(success=False)
            while response.success == False:
                try:
                    response = stub.AppendEntries(
                        chainreplication_pb2.AppendEntriesRequest(
                        seqnum=request.seqnum, command=request.command,
                        key=request.key, value=request.value, client=request.client))
                except Exception as e:
                    time.sleep(0.5)
                    continue
            # TODO
            # self.sentQueue.append()
        return

    def AppendEntries(self, request:chainreplication_pb2.AppendEntriesRequest, context):
        # commit to db
        self.db.Put(bytearray(request.key, encoding="utf8"), bytearray(request.value, encoding="utf8"))

        # send to peer in FIFO order
        # threadpoolexecutor uses python's SimpleQueue internally
        self.crthreadpool.submit(self.__appendEntries, request)

        # if tail, send response to client
        # TODO
            

        return chainreplication_pb2.AppendEntriesResponse(success=True)
    
    def Get(self, request, context):
        # TODO
        return database_pb2.GetResponse(value='')
    
    def Put(self, request, context):
        # TODO
        return database_pb2.PutResponse(errormsg='')

    def setupDB(self, id):
        '''
        Helper method to set up levelDB
        '''
        dbpath = './{}_db'.format(id)
        from shutil import rmtree
        rmtree(dbpath, ignore_errors=True)
        self.db = leveldb.LevelDB(dbpath)


if __name__ == "__main__":
    pass
