sudo apt update
sudo apt -y install python3-pip
sudo apt -y install default-jdk
sudo apt -y install default-jre

pip3 install grpcio
pip3 install grpcio-tools
pip3 install kazoo
pip3 install leveldb

pushd ~
wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz
tar -xvf apache-zookeeper-3.7.1-bin.tar.gz
popd