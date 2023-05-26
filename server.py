from kazoo.client import KazooClient
from kazoo.protocol.states import EventType

import leveldb
import grpc
import chainreplication_pb2
import chainreplication_pb2_grpc
import database_pb2
import database_pb2_grpc

from enum import Enum
from collections import deque
import time
import signal
from sys import exit
import threading
from concurrent import futures
import logging

logLevel = logging.DEBUG        # update log level to display appropriate logs to console

def getLogger(source):
    logger = logging.getLogger(source)
    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)
    logger.setLevel(logLevel)
    return logger

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
        logger = getLogger(self.__init__.__qualname__)

        # Current tail notices when a new node is created in ZK
        # It starts propagating new reqs to the new node. 
        # This is queued up until the synchroniztion (i.e bootstrap) is completed
        self.bootstrap = True
        
        self.mycrnodeport = crNodePort        # my CR node port

        # set up zookeeper
        self.zk = KazooClient(hosts='127.0.0.1:2181')
        self.zk.start()
        logger.debug("Started Zookeeper Client")

        # ensure necessart zk paths
        self.zk.ensure_path("/cluster")
        self.zk.ensure_path("/tail")
        self.zk.ensure_path("/head")

        # instantiate levelDB
        self.setupDB(crNodePort.split(':')[1])
        logger.debug("LevelDB set up")

        self.crthreadpool = futures.ThreadPoolExecutor(max_workers=1)

        # start chain replication grpc server thread
        self.crthread = threading.Thread(target=self.__chainReplicationListener, args=(crNodePort, ))
        self.crthread.start()
        
        # get peers from zk
        peers = self.zk.get_children(path="/cluster")

        # let peers know of my existence
        self.myzknode = self.zk.create(path="/cluster/guid_", value=crNodePort.encode(), ephemeral=True, sequence=True).split('/')[2]
        logger.info("Zookeeper cluster updated, mynode = " + self.myzknode)

        if len(peers) == 0:
            logger.info("I am the only server")
            self.head = crNodePort
            self.tail = crNodePort
            
            self.zk.set("/head", crNodePort.encode())
            logger.info("ZK head node updated")
        else:
            # sync from curr tail
            self.syncWithCurrentTail(max(peers))
            logger.info("Sync completed with current tail")
            
            self.tail = crNodePort
        
        self.zk.set("/tail", crNodePort.encode())
        logger.info("ZK tail node updated")

        # start gRPC database server thread
        self.dbthread = threading.Thread(target=self.__databaseListener, args=(dbNodePort, ))
        self.dbthread.start()

        self.peer = ""                           # peer is initially none since i'm the tail
        self.sentQueue = deque()                 # sent queue contains entries sent to peer not acked by tail

        # setup ZK watchers
        self.zk.get("/tail", watch=self.tailChanged)
        self.zk.get("/head", watch=self.tailChanged)
        self.zk.get_children("/cluster", watch=self.clusterChanged)
        logger.debug("ZK Watchers enabled")

        signal.signal(signal.SIGINT, self.signalHandler)

        # needed to ensure new data is committed to disk only after synchronization (i.e. bootstrap) is completed
        self.bootstrap = False

        return

    def tailChanged(self, event):
        # update tail when changed
        logger = getLogger(self.tailChanged.__qualname__)
        if event.type == EventType.CHANGED:
            self.tail = self.zk.get("/tail")[0].decode()
            logger.info("Tail updated to " + self.tail)
    
    def headChanged(self, event):
        # update head when changed
        logger = getLogger(self.headChanged.__qualname__)
        if event.type == EventType.CHANGED:
            self.head = self.zk.get("/head")[0].decode()
            logger.info("Head updated to " + self.head)
    
    def clusterChanged(self, event):
        # update peer when changed
        logger = getLogger(self.clusterChanged.__qualname__)
        
        if event.type == EventType.CHILD:
            peers = self.zk.get_children("/cluster")
            try:
                mynextnode = next(pnode for pnode in peers if pnode > self.myzknode)
                peernodeport = self.zk.get("/cluster/" + mynextnode)[0].decode()
            except StopIteration as e:
                peernodeport = ""
            
            if self.peer != peernodeport:
                logger.info("Peer updated to " + peernodeport)

            self.peer = peernodeport
            # TODO how to send missed updates when a node crashes?

    def __del__(self):
        self.zk.stop()
    
    def __chainReplicationListener(self, crNodePort):
        # listen for chain replication requests
        logger = getLogger(self.__chainReplicationListener.__qualname__)
        self.crserver = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        chainreplication_pb2_grpc.add_ChainReplicationServicer_to_server(self, self.crserver)
        self.crserver.add_insecure_port(crNodePort)
        self.crserver.start()
        logger.info("Chain replication server started, listening on " + crNodePort)
        self.crserver.wait_for_termination()
    
    def __databaseListener(self, dbNodePort):
        # listen for DB requests
        logger = getLogger(self.__databaseListener.__qualname__)
        self.dbserver = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        database_pb2_grpc.add_DatabaseServicer_to_server(self, self.dbserver)
        self.dbserver.add_insecure_port(dbNodePort)
        self.dbserver.start()
        logger.info("Database server started, listening on " + dbNodePort)
        self.dbserver.wait_for_termination()

    def syncWithCurrentTail(self, tailzknode):
        logger = getLogger(self.syncWithCurrentTail.__qualname__)

        # get current tail
        tailNodePort = self.zk.get("/cluster/"+tailzknode)[0]

        # send sync request to current tail
        with grpc.insecure_channel(tailNodePort) as channel:
            stub = chainreplication_pb2_grpc.ChainReplicationStub(channel)
            response = chainreplication_pb2.SyncResponse(success=False, entries=[])
            while response.success == False:
                try:
                    response = stub.Sync(chainreplication_pb2.SyncRequest())
                except Exception as e:
                    logger.error("GRPC exception" + str(e))
                    time.sleep(0.5)
                    continue 
        logger.info("Response from grpc received!")

        # commit response entries to levelDB
        for e in response.entries:
            self.db.Put(bytearray(e.key, encoding="utf8"), bytearray(e.value, encoding="utf8"))
        
        logger.info("LevelDB synced!")

        return

    def Sync(self, request, context):
        # handle sync request from new tail
        logger = getLogger(self.Sync.__qualname__)
        logger.info("Sync request received")

        # get all data from levelDB
        itr = self.db.RangeIter()
        data = []
        for key, value in itr:
            data.append(chainreplication_pb2.KVPair(key=key, value=value))

        logger.info("Sending fetched data from LevelDB...")

        # send it to new tail
        return chainreplication_pb2.SyncResponse(success=True, entries=data)
    
    def __appendEntries(self, request:chainreplication_pb2.AppendEntriesRequest):
        # helper thread to propagate entries down the chain
        logger = getLogger(self.__appendEntries.__qualname__)

        # I am the tail, no need to append
        if self.peer == "":
            return
        
        logger.info("Appending entries to "+ self.peer)

        # append entries to peer
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
                    logger.error("GRPC exception " + str(e))
                    time.sleep(0.5)
                    continue
        
        logger.info("Append Entries successful")
            # TODO
            # self.sentQueue.append()
        return

    def AppendEntries(self, request:chainreplication_pb2.AppendEntriesRequest, context):

        # handle incoming append entries
        logger = getLogger(self.AppendEntries.__qualname__)
        while self.bootstrap == True:
            time.sleep(0.5)
        
        logger.info("AppendEntries request received")

        # commit to db
        self.db.Put(bytearray(request.key, encoding="utf8"), bytearray(request.value, encoding="utf8"))

        logger.info("Commited entry")

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
                        logger.error("GRPC exception " + str(e))
                        time.sleep(0.5)
            
            logger.info("Sent put response to client")
    
        return chainreplication_pb2.AppendEntriesResponse(success=True)
    
    def Get(self, request, context):
        # handle incoming get requests

        logger = getLogger(self.Get.__qualname__)
        
        # respond only if tail
        if self.tail == self.mycrnodeport:
            logger.info("Get request received")
            val = self.db.Get(bytearray(request.key, 'utf-8')).decode()
            return database_pb2.GetResponse(error="", value=val)
        
        logger.debug("Get request sent to non-tail server")

        return database_pb2.GetResponse(error="contact tail", value="")
    
    def Put(self, request, context):
        # handle incoming put requests
        logger = getLogger(self.Put.__qualname__)

        # process only if head
        if self.head == self.mycrnodeport:
            logger.info("Put request received")

            self.AppendEntries(chainreplication_pb2.AppendEntriesRequest(seqnum=request.seqnum, command="PUT", key=request.key, value=request.value, client=request.client), None)
            return database_pb2.PutResponse(error="")
        
        logger.debug("Put request sent to non-head server")

        return database_pb2.PutResponse(error="contact head")

    def setupDB(self, uid):
        '''
        Helper method to set up levelDB
        '''
        dbpath = './{}_db'.format(uid)
        from shutil import rmtree
        rmtree(dbpath, ignore_errors=True)
        self.db = leveldb.LevelDB(dbpath)
    
    def signalHandler(self, sig, frame):
        getLogger(self.signalHandler.__qualname__).info('Server exiting!!')
        self.zk.stop()
        exit(0)

if __name__ == "__main__":
    # parse CLI inputs
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-dnp', '--dbnodeport', help='Database NodePort', required=True)
    parser.add_argument('-cnp', '--crnodeport', help='Chain Replication NodePort', required=True)
    args = parser.parse_args()

    server = ChainReplicator(args.dbnodeport, args.crnodeport)

    signal.pause()

