# chain_replication
Strongly consistent system with simple fault recovery, high availability and high throughput. 

## Dependencies
See setup.sh

## Building protos
```sh
python3 -m grpc_tools.protoc -I ./protos --python_out=. --pyi_out=. --grpc_python_out=. ./protos/*
```

## Running the server
Start zookeeper at default port 2181
```sh
cd ~/apache-zookeeper-3.7.1-bin/
./bin/zkServer.sh start
```

Run server using the command
```sh
python3 server.py --dbnodeport localhost:50051 --crnodeport localhost:50052
# other servers
python3 server.py --dbnodeport localhost:50061 --crnodeport localhost:50062
python3 server.py --dbnodeport localhost:50071 --crnodeport localhost:50072
python3 server.py --dbnodeport localhost:50081 --crnodeport localhost:50082
```