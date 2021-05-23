## Rocksdb源码安装

| 作者 | 时间 |QQ技术交流群 |
| ------ | ------ |------ |
| perrynzhou@gmail.com |2021/05/23 |672152841 |

####  下载rocksdb源码

```
wget  https://github.com/facebook/rocksdb/archive/refs/tags/v6.20.3.tar.gz
```

#### 安装依赖包

- ubuntu/debian
```
apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
```
-  centos
```
git clone https://github.com/gflags/gflags.git
cd gflags
git checkout v2.0
./configure && make && sudo make install

yum install epel-release -y && yum cleanall && yum makecache
yum install snappy snappy-devel zlib zlib-devel  bzip2 bzip2-devel  lz4-devel  libasan libzstd-devel libzstd-devel -y
```

#### 编译

```
cd rocksdb-6.20.3/ && mkdir build  && cd build && cmake .. 

// 这种方式是默认的debug模式
cd rocksdb-6.20.3/ && make && make install

// 静态库的release模式
cd rocksdb-6.20.3/ && mkdir build  && cd build && cmake .. 
&&make static_lib  && make install

// 共享库的release模式
cd rocksdb-6.20.3/ && mkdir build  && cd build && cmake .. 
&&make shared_lib  && make install

```


