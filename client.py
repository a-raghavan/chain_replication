import grpc
import database_pb2
import database_pb2_grpc
import threading
from concurrent import futures
import time

class DB(database_pb2_grpc.DatabaseServicer):
    def __dblistener(self):
        self.dbserver = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        database_pb2_grpc.add_DatabaseServicer_to_server(self, self.dbserver)
        self.dbserver.add_insecure_port("localhost:55555")
        self.dbserver.start()
        self.dbserver.wait_for_termination()

    def __init__(self):
        self.seqnum = 0
        self.dbthead = threading.Thread(target=self.__dblistener, args=())
        self.dbthead.start()
    
    def put(self, k, v):
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = database_pb2_grpc.DatabaseStub(channel)
            self.seqnum += 1
            response = stub.Put(database_pb2.PutRequest(seqnum=self.seqnum, key=k, value=v, client="localhost:55555"))
    
    def PutResult(self, request, context):
        print("Put successful, seqnum - ", request.seqnum)
        return database_pb2.PutResultResponse()

    def get(self, k):
        with grpc.insecure_channel("localhost:50071") as channel:
            stub = database_pb2_grpc.DatabaseStub(channel)
            response = stub.Get(database_pb2.GetRequest(key=k))
            print("Get successful, value = ", response.value)


if __name__ == "__main__":
    import random
    db = DB()
    for i in range(100):
        db.put("raghavan", str(random.randint(1, 1000)))
        time.sleep(0.5)
    print("Done!")
    db.dbthead.join()
    