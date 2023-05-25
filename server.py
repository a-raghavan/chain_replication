from kazoo.client import KazooClient
import leveldb
import queue
import threading
from concurrent import futures
import grpc
import chainreplication_pb2
import chainreplication_pb2_grpc
import database_pb2
import database_pb2_grpc

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
            self.syncWithCurrentTail()
            self.tail = nodeport
        
        self.zk.set("/cluster/tail", nodeport.encode())

        
        # start gRPC database server thread
        self.dbthread = threading.Thread(self.__databaseListener)
        
        self.peer = None                         # peer is initially none since i'm the tail
        
        self.queue = queue.Queue()               # instantiate queue

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

    def syncWithCurrentTail(self):
        # TODO
        return

    def Sync(self, request, context):
        # TODO
        return chainreplication_pb2.SyncResponse(success=True)
    
    def AppendEntries(self, request, context):
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
