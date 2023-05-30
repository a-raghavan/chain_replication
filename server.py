from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.recipe.watchers import ChildrenWatch

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

# TODO:
# 1. Testing
# 2. apportioned queries

logLevel = logging.DEBUG          # update log level to display appropriate logs to console

def getLogger(source):
    logger = logging.getLogger(source)
    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)
    logger.setLevel(logLevel)
    return logger

class ChainReplicator(chainreplication_pb2_grpc.ChainReplicationServicer, database_pb2_grpc.DatabaseServicer):

    def __init__(self, dbNodePort:str, crNodePort:str) -> None:
        logger = getLogger(self.__init__.__qualname__)

        # Current tail notices when a new node is created in ZK
        # It starts propagating new reqs to the new node. 
        # This is queued up until the synchroniztion (i.e bootstrap) is completed
        self.bootstrap = True

        # Used to serialize appendEntries during crash failures
        self.crashrecovery = False
        
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
        self.dblock = threading.Lock()
        logger.debug("LevelDB set up")

        self.crthreadpool = futures.ThreadPoolExecutor(max_workers=1)
        self.ackthreadpool = futures.ThreadPoolExecutor(max_workers=1)

        # start chain replication grpc server thread
        self.crthread = threading.Thread(target=self.__chainReplicationListener, args=(crNodePort, ))
        self.crthread.start()
        
        # get peers from zk
        peers = self.zk.get_children(path="/cluster")

        # let peers know of my existence
        self.myzknode = self.zk.create(path="/cluster/guid_", value=crNodePort.encode(), ephemeral=True, sequence=True).split('/')[2]
        logger.info("Zookeeper cluster updated, mynode = " + self.myzknode)

        if len(peers) == 0:
            # first node
            logger.info("I am the only server")
            self.zk.set("/head", crNodePort.encode())
            logger.info("ZK head node updated")
        else:
            # sync from curr tail
            self.syncWithCurrentTail(max(peers))
            logger.info("Sync completed with current tail")
        
        self.zk.set("/tail", crNodePort.encode())
        logger.info("ZK tail node updated")

        self.next = ""
        self.prev = ""
        
        # sent queue contains entries sent to peer not acked by tail
        self.sentQueue = deque()

        # setup ZK watchers and update self.next && self.prev
        self.tailwatcher = DataWatch(self.zk, "/tail", self.tailChanged)
        self.headwatcher = DataWatch(self.zk, "/head", self.headChanged)
        self.clusterwatcher = ChildrenWatch(self.zk, "/cluster", self.clusterChanged)
        logger.debug("ZK Watchers enabled")

        # start gRPC database server thread
        self.dbthread = threading.Thread(target=self.__databaseListener, args=(dbNodePort, ))
        self.dbthread.start()

        signal.signal(signal.SIGINT, self.signalHandler)

        # needed to ensure new data is committed to disk only after synchronization (i.e. bootstrap) is completed
        self.bootstrap = False

        return

    def tailChanged(self, data, stat):
        # update tail when changed
        logger = getLogger(self.tailChanged.__qualname__)
        self.tail = data.decode()
        logger.info("Tail updated to "+ data.decode())
    
    def headChanged(self, data, stat):
        # update head when changed
        logger = getLogger(self.headChanged.__qualname__)
        self.head = data.decode()
        logger.info("Head updated to "+ data.decode())
    
    def clusterChanged(self, children):
        # update peer when changed
        logger = getLogger(self.clusterChanged.__qualname__)
        
        peers = sorted(children)
        currNodeIdx, currNode  = next( (idx, pnode) for (idx, pnode) in enumerate(peers) if pnode == self.myzknode)
        
        # update next peer
        if currNodeIdx == len(peers)-1:
            nextpeernodeport = ""
        else:
            mynextnode = peers[currNodeIdx+1]
            nextpeernodeport = self.zk.get("/cluster/" + mynextnode)[0].decode()
        
        # update prev peer
        if currNodeIdx == 0:
            prevpeernodeport = ""
        else:
            myprevnode = peers[currNodeIdx-1]
            prevpeernodeport = self.zk.get("/cluster/" + myprevnode)[0].decode()
        
        logger.info("Prev peer is now - " + prevpeernodeport)
        logger.info("Next peer is now - " + nextpeernodeport)

        nextcrashed = (self.next != nextpeernodeport)

        # update head and tail if changed
        if prevpeernodeport == "" and self.prev != prevpeernodeport:
            self.zk.set("/head", self.mycrnodeport.encode())

        if nextpeernodeport == "" and self.next != nextpeernodeport:
            self.zk.set("/tail", self.mycrnodeport.encode())

        self.next = nextpeernodeport
        self.prev = prevpeernodeport
        
        # Handle failures
        if nextcrashed:
            logger.info("Next failed, resending items in sent queue...")
            self.crashrecovery = True
            while len(self.sentQueue) != 0:
                self.crthreadpool.submit(self.__appendEntries, self.sentQueue.popleft())
                #self.AppendEntries(self.sentQueue.popleft(), None)
            self.crashrecovery = False
            logger.info("Resend successful")

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

        # TODO fetch latest tail nodeport upon GRPC failure

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
        while self.bootstrap == True:
            time.sleep(0.5)
        
        # handle sync request from new tail
        logger = getLogger(self.Sync.__qualname__)
        logger.info("Sync request received")

        # get all data from levelDB
        with self.dblock:
            itr = self.db.RangeIter()
            data = []
            for key, value in itr:
                data.append(chainreplication_pb2.KVPair(key=key.decode(), value=value.decode()))

        logger.info("Sending fetched data from LevelDB...")

        # send it to new tail
        return chainreplication_pb2.SyncResponse(success=True, entries=data)
    
    def __appendEntries(self, request:chainreplication_pb2.AppendEntriesRequest):
        # helper thread to propagate entries down the chain
        logger = getLogger(self.__appendEntries.__qualname__)

        # I am not the tail
        if self.next != "":
            logger.info("Appending entries to "+ self.next)

            # append entries to peer
            response = chainreplication_pb2.AppendEntriesResponse(success=False)
            while response.success == False:
                # self.next may change to server crashes. 
                # Hence, it is essential that self.next is within the while loop
                with grpc.insecure_channel(self.next) as channel:
                    stub = chainreplication_pb2_grpc.ChainReplicationStub(channel)
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
        
        self.sentQueue.append(chainreplication_pb2.AppendEntriesRequest(
                        seqnum=request.seqnum, command=request.command,
                        key=request.key, value=request.value, client=request.client))
        logger.debug("Added entry to sent queue")

        # i am the tail, need to respond to client and initiate ACK flow
        if self.next == "":
            with grpc.insecure_channel(request.client) as channel:
                stub = database_pb2_grpc.DatabaseStub(channel)
                # what happens if client crashes? maybe try like 10 times and exit?
                for _ in range(10):
                    try:
                        stub.PutResult(database_pb2.PutResultRequest(seqnum=request.seqnum, success=True))
                        break
                    except Exception as e:
                        logger.error("GRPC exception " + str(e))
                        time.sleep(0.5)
            
            logger.info("Sent put response to client")
            
            self.ackthreadpool.submit(self.Ack, chainreplication_pb2.AckRequest(seqnum=request.seqnum), None)
            logger.info("Submitted ACK job to threadpool")
        
        return

    def AppendEntries(self, request:chainreplication_pb2.AppendEntriesRequest, context):

        # handle incoming append entries
        logger = getLogger(self.AppendEntries.__qualname__)
        while self.bootstrap == True or self.crashrecovery == True:
            time.sleep(0.5)
        
        logger.info("AppendEntries request received")

        # commit to db
        with self.dblock:
            self.db.Put(bytearray(request.key, encoding="utf8"), bytearray(request.value, encoding="utf8"))

        logger.info("Commited entry")

        # send to peer in FIFO order
        # threadpoolexecutor uses python's SimpleQueue internally
        self.crthreadpool.submit(self.__appendEntries, request)
    
        return chainreplication_pb2.AppendEntriesResponse(success=True)
    
    def Ack(self, request, context):
        logger = getLogger(self.Ack.__qualname__)
        #print("AAAA", self.sentQueue[0].seqnum,  request.seqnum)
        if self.sentQueue[0].seqnum == request.seqnum:
            logger.critical("Received Ack for a request not in queue front :(")
            # maybe an old RPC reaching the server??
            return chainreplication_pb2.AckResponse()
        
        self.sentQueue.popleft()
        logger.debug("Removed completed update request from sent queue")
        
        if self.prev == "":
            # I am head
            return chainreplication_pb2.AckResponse()           # doesnt matter
        
        while True:
            # self.prev may change to server crashes. 
            # Hence, it is essential that self.prev is within the while loop
            with grpc.insecure_channel(self.prev) as channel:
                stub = chainreplication_pb2_grpc.ChainReplicationStub(channel)
                try:
                    stub.Ack(chainreplication_pb2.AckRequest(seqnum=request.seqnum))
                    break
                except Exception as e:
                    logger.error("GRPC exception " + str(e))
                    time.sleep(0.5)
            
        logger.info("Ack successful to prev peer")

        return chainreplication_pb2.AckResponse()

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
        # Helper method to set up levelDB

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

