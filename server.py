from kazoo.client import KazooClient
import kazoo.exceptions as zke
from kazoo.protocol.states import EventType
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
import signal
from sys import exit

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

    def __init__(self, dbNodePort:str, crNodePort:str) -> None:
        self.bootstrap = True
        
        self.mycrnodeport = crNodePort        # my CR node port

        # set up zookeeper
        self.zk = KazooClient(hosts='127.0.0.1:2181')
        self.zk.start()

        # ensure necessart zk paths
        self.zk.ensure_path("/cluster")
        self.zk.ensure_path("/tail")
        self.zk.ensure_path("/head")

        # instantiate levelDB
        self.db = self.setupDB(id)      

        self.crthreadpool = futures.ThreadPoolExecutor(max_workers=1)

        # start chain replication grpc server thread
        self.crthread = threading.Thread(self.__chainReplicationListener, crNodePort)
        
        # Current tail notices when a new node is created in ZK
        # It starts propagating new reqs to the new node. 
        # This is queued up until the synchroniztion is completed

        self.myzknode = self.zk.create(path="/cluster/guid_", value=crNodePort.encode(), ephemeral=True, sequence=True)

        peers = self.zk.get_children(path="/cluster")
        if len(peers) == 0:
            self.head = crNodePort
            self.tail = crNodePort
            self.zk.set("/head", crNodePort.encode())
        else:
            # sync from curr tail
            self.syncWithCurrentTail(max(peers))
            self.tail = crNodePort
        
        self.zk.set("/tail", crNodePort.encode())

        # start gRPC database server thread
        self.dbthread = threading.Thread(self.__databaseListener, dbNodePort)
        
        self.peer = ""                           # peer is initially none since i'm the tail
        
        self.sentQueue = deque()                 # sent queue contains entries sent to peer not acked by tail

        self.zk.get_children("/cluster", watch=self.clusterChanged)

        signal.signal(signal.SIGINT, self.signalHandler)

        # needed to ensure new data is committed to disk only after synchronization (i.e. bootstrap) is completed
        self.bootstrap = False
 
        return

    def clusterChanged(self, event):
        if event.type == EventType.CHILD:
            peers = self.zk.get_children("/cluster")

            mynextnode = next(pnode for pnode in peers if pnode > self.mycrnodeport)
            peernodeport = self.zk.get("/cluster/" + mynextnode)[0]

            self.peer = peernodeport
            
            # TODO how to send missed updates when a node crashes?

    def __del__(self):
        self.zk.stop()
    
    def __chainReplicationListener(self, crNodePort):
        self.crserver = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        chainreplication_pb2_grpc.add_ChainReplicationServicer_to_server(self, self.crserver)
        self.crserver.add_insecure_port(crNodePort)
        self.crserver.start()
        print("Chain replication server started, listening on " + crNodePort)
        self.crserver.wait_for_termination()
    
    def __databaseListener(self, dbNodePort):
        self.dbserver = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        database_pb2_grpc.add_DatabaseServicer_to_server(self, self.dbserver)
        self.dbserver.add_insecure_port(dbNodePort)
        self.dbserver.start()
        print("Database server started, listening on " + dbNodePort)
        self.dbserver.wait_for_termination()

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
        while self.bootstrap == True:
            time.sleep(0.5)
        
        # commit to db
        self.db.Put(bytearray(request.key, encoding="utf8"), bytearray(request.value, encoding="utf8"))

        # send to peer in FIFO order
        # threadpoolexecutor uses python's SimpleQueue internally
        self.crthreadpool.submit(self.__appendEntries, request)

        # if tail, send response to client
        if self.tail == self.mycrnodeport:
            with grpc.insecure_channel(request.client) as channel:
                stub = database_pb2_grpc.DatabaseStub(channel)
                while True:
                    try:
                        stub.PutResult(database_pb2.PutResultRequest(seqnum=request.seqnum, success=True))
                        break
                    except Exception as e:
                        time.sleep(0.5)

        return chainreplication_pb2.AppendEntriesResponse(success=True)
    
    def Get(self, request, context):
        # respond only if tail
        if self.tail == self.mycrnodeport:
            val = self.db.Get(bytearray(request.key, 'utf-8')).decode()
            return database_pb2.GetResponse(error="", value=val)
        return database_pb2.GetResponse(error="contact tail", value="")
    
    def Put(self, request, context):
        if self.head == self.mycrnodeport:
            self.AppendEntries(chainreplication_pb2.AppendEntriesRequest(seqnum=request.seqnum, command="PUT", key=request.key, value=request.value, client=context.peer()))
            return database_pb2.PutResponse(error="")
        return database_pb2.PutResponse(error="contact head")

    def setupDB(self, id):
        '''
        Helper method to set up levelDB
        '''
        dbpath = './{}_db'.format(id)
        from shutil import rmtree
        rmtree(dbpath, ignore_errors=True)
        self.db = leveldb.LevelDB(dbpath)
        return
    
    def signalHandler(self, sig, frame):
        print('Server exiting!!')
        self.crserver.stop()
        self.dbserver.stop()
        self.crthreadpool.shutdown(cancel_futures=True)
        self.zk.stop()
        exit(0)



if __name__ == "__main__":
    # parse CLI inputs
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-dnp', '--dbnodeport', help='Database NodePort', required=True)
    parser.add_argument('-cnp', '--crnodeport', help='Chain Replication NodePort', required=True)
    #parser.add_argument('-n', '--nodes', nargs='*', help='NodePorts of all servers (space separated). e.g. -n 10.0.0.1:5001 10.0.0.1:5002 10.0.0.1:5003', required=True)
    args = parser.parse_args()

    server = ChainReplicator(args.dbnodeport, args.crnodeport)

    signal.pause()

