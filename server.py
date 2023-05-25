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

class ChainReplicator:

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
        
        self.peer = None                         # peer is initially none since i'm the tail
        
        self.queue = queue.Queue()               # instantiate queue

        self.bootstrap = False
 

        return

    def __del__(self):
        self.zk.stop()
    
    def syncWithCurrentTail(self):
        
        return

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
