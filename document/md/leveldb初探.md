### leveldb 数据查找流程

| 作者 | 时间 |QQ技术交流群 |
| ------ | ------ |------ |
| perrynzhou@gmail.com |2020/12/01 |672152841 |

#### Leveldb基本概念

- memtable:保存在内存中的kv,leveldb有两种memtable,一种是可读写的memtable,一种是只读的memtable,两种的memtable在内存中都是以skiplist呈现，当应用在memtable中都是构造LookupKey中的memtable_key()方法获取查询key,然后在内存中进行查找
- log:leveldb中任何的写操作首先是写入wal log,log是把随机写转换为顺序写的方式，数据裸盘后，在写memtable这样可以防止数据丢失。这个log也是以block形式组织的
- block：leveldb磁盘数据都是以block形式组织，sstable中每层的文件都是以block形式组织
- sstable:key和value在磁盘上的呈现形式，这些数据都是只读。默认情况下sstable维护了7层的文件


#### Open数据库
- 根据DB文件路径初始化内部实例
- Recovery整个数据库
- 

#### Get流程
- 根据应用程序输入的key,构造LookupKey，整个LookupKey包含了三个类型的Key,一个是memtable key用于memtable中查找value;一个是internal key整个是在磁盘上的sstable中查找；最后一个就是应用输入的user key.LookupKey中定义的三种key分别用于客户端输入的key、memtable中查找时候用到的key、sstable中查找时候用的key

  ![lookupkey](../images/lookupkey.jpg)

```
// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  //应用程序输入的key,拷贝到kstart_中
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  //kValueTypeForSeek表示非删除的标记，PackSequenceAndType用于编码SequenceNumber和valuetype.EncodeFixed64这把SequenceNumber和valuetype编码后的数据按照一定编码方式写入到kstart_中
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

  ~LookupKey();

  // 根据应用输入的key，返回在memtable中查找使用的key.格式为：可变部分(从1个字节到5个字节不等)+user key+SequenceNumber+ValueType
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // 根据应用输入的key，返回在sstable中查找用户的key,格式为user key+SequenceNumber+ValueType
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // 这个就是根据应用输入的key来构造和应用输入key一样的key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // 记录从可变部分的地址
  const char* start_;
  //记录user key的地址
  const char* kstart_;
  // 构造key的最后一个字符串的地址
  const char* end_;
  //为了减少小key的allcation而优化的
  char space_[200];  
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

} 
```
- 构造LookupKey后需要在读写的memtable中进行第一次查找，如果找到就返回;如果么有找到就在只读的memtable中进行查找，如果找到就返回;如果还是没有找到则需要从sstable第0层依次进行查找，如果找到就返回；反之就返回无这个key对应的value.如果leveldb中最坏情况需要读2次内存和多次文件，这样性能是比较差的

```

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  // 获取当前可读写的memtable
  MemTable* mem = mem_;
  // 获取当前只读的memtable
  MemTable* imm = imm_;
  // 这里封装了sstable中每层文件的的操作，包括每层每个节点对应的文件等信息，这里主要是为后续在sstable中查找key对应的value做准备，万一在memtable都没有，最后只能在这里查找了。讲道理这里可以加一层布隆过滤器，效率会更好
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // 首先是在内存中查找时候构造LookupKey后续都可以在memtable中和sstable中查找
    LookupKey lkey(key, snapshot);
    // 首先在可读写的memtable中查找，这里用到的mem_key
    if (mem->Get(lkey, value, &s)) {
      // Done
    // 如果只读memtable为空，mem->Get失败后则在只读memtable中查找
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
     // 在当前sstable中从0层开始开始往下找key对应的value
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
  	// 根据条件触发是否要做compaction操作
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}
```
- 应用程序代码示例
```
  // Open a database.
    leveldb::DB* db;
    leveldb::Options opts;
    opts.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(opts, "./testdb", &db);
    assert(status.ok());
    
    // Read data.
    std::string val;
    status = db->Get(leveldb::ReadOptions(), "name", &val);
    assert(status.ok());
    std::cout << val << std::endl;
```
