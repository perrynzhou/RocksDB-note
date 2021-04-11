## LevelDB Put如何写入数据

| 作者 | 时间 |QQ技术交流群 |
| ------ | ------ |------ |
| perrynzhou@gmail.com |2020/12/01 |672152841 |

### leveldb插入数据步骤

- 用户输入的kv数据首先组装为wal log entry写入到wal log中
- 然后将kv数据从内存中的wal log entry解析为memtable中的数据
- 最后在插入到memtable中，完成此次数据的put操作

### 写入流程分析
- DB::Put分析

```
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  // 拼接key和value
  batch.Put(key, value);
  // 写入wal日志和memtable
  return Write(opt, &batch);
}
```
-  WriteBatch::Put 分析
```
void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // 写入value类型，标记为非删除
  rep_.push_back(static_cast<char>(kTypeValue));
  // 组装写入的key和value
  // 按照32位来编码key的大小和key的内存
  PutLengthPrefixedSlice(&rep_, key);
  // 按照32位来编码value的大小和value内容
  PutLengthPrefixedSlice(&rep_, value);
  // 组织后的格式为： |value_type|key_size|key||value_size|value|
}
```

- DBImpl::Write 分析

```
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
		// 这里是组装wal log格式写入到Wal日志
		// 每个wal log entry格式为: |checksum|record length|record type|data|
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
if (status.ok()) {
			// 这里是把此次write_batch内容插入到memtable中
			// 解析write_batch中的key和value，插入到当前memtable中
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
}
```
### leveldb写入流程调试
- leveldb 下载
```
git clone --recurse-submodules https://github.com/google/leveldb.git
mkdir -p build && cd build
// cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .
```
- 客户端程序
```
#include <iostream>
#include <cassert>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

int main()
{
    // Open a database.
    leveldb::DB* db;
    leveldb::Options opts;
    opts.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(opts, "./testdb", &db);
    assert(status.ok());
    
    // Write data.
    status = db->Put(leveldb::WriteOptions(), "name", "jinhelin");
    assert(status.ok());


    // Batch atomic write.
    leveldb::WriteBatch batch;
    batch.Delete("name");
    batch.Put("name0", "jinhelin0");
    batch.Put("name1", "jinhelin1");
    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());

    // Scan database.
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << ": " << 
          it->value().ToString() << std::endl;
    }
    assert(it->status().ok());
    
    // Range scan, example: [name3, name8)
    for (it->Seek("name3"); 
         it->Valid() && it->key().ToString() < "name8"; 
         it->Next()) {
        std::cout << it->key().ToString() << ": " << 
          it->value().ToString() << std::endl;
    } 

}

```

- 编译客户端和调试

```
g++ -ggdb3 -O0 example.cc  -o test -lleveldb -lpthread
gdb ./test
(gdb) info break
Num     Type           Disp Enb Address            What
1       breakpoint     keep y   0x000000000040a756 in leveldb::DB::Open(leveldb::Options const&, std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const&, leveldb::DB**) at /root/debug/leveldb/db/db_impl.cc:1484
        breakpoint already hit 1 time
2       breakpoint     keep y   0x0000000000409344 in leveldb::DBImpl::Put(leveldb::WriteOptions const&, leveldb::Slice const&, leveldb::Slice const&) 
                                                   at /root/debug/leveldb/db/db_impl.cc:1193
3       breakpoint     keep y   0x00000000004047e9 in leveldb::DBImpl::NewDB() at /root/debug/leveldb/db/db_impl.cc:182
4       breakpoint     keep y   0x00000000004041af in leveldb::DBImpl::DBImpl(leveldb::Options const&, std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const&) at /root/debug/leveldb/db/db_impl.cc:150
```
