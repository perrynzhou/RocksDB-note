## leveldb源码剖析-写入流程分析

| 作者 | 时间 |QQ技术交流群 |
| ------ | ------ |------ |
| perrynzhou@gmail.com |2020/12/01 |672152841 |

### 客户端写入代码

```
int main()
{
    // 打开数据库
    leveldb::DB* db;
    // 初始化leveldb的memtable内存大小、文件操作接口、block大小、打开文件数限制等
    leveldb::Options opts;
    opts.create_if_missing = true;
    // 打开数据库操作
    leveldb::Status status = leveldb::DB::Open(opts, "./testdb", &db);
    assert(status.ok());
    
    // 写入一个kv流程
    status = db->Put(leveldb::WriteOptions(), "name", "jinhelin");
    assert(status.ok());
 }
```



### Put源码分析

- Leveldb::DB定义了leveldb的数据库操作的所有的接口，都是纯虚函数，方便后续定义其实现DBImpl
- db->Put具体的实现是由DBImpl具体实现类的Put方式实现，Put操作遵循wal的协议，先写日志在写数据，Wal 日志是以block方式组织，每条的kv item也是按照一定的格式来存储，总体的写入过程实现参见如下
```
// leveldb::DB *db的实现是DBImpl，db->put方法也是调用DBImpl中的put方法
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
 	// 定义leleldb中的磁盘写入的writer，先按照kv item组织单个put操作的kv结构，这个结构一般是kv item的格式。
  WriteBatch batch;
	// kv item的log的格式为：|seq num|kv item count|key type|key size|key value|value size| value|key size|
	// batch.Put是按照用户输入的key和val按照一定的格式组织，按照写入到batch的rep_这个字符串中
  batch.Put(key, value);
  // Write方法是以block组织方式wal log,把多个batch的数据合并到一个batch中，然后写入到wal中，写入成功以后再次把这个batch解析，存储到memtable，到这里leveldb的写流程已经结束，但是写入过程中需要搞清楚writebatch到底是什么？都做了什么事情
  return Write(opt, &batch);
}
```
- WriteBatch是用来存储leveldb内部的key和value来用的，后续如果是多个put操作，会把后续除过第一个的kv统一合并为一个writebatch来写入到wal log，如下仅仅罗列了部分接口其定义
```
class LEVELDB_EXPORT WriteBatch {
 public:
 // 定义写完wal log后往memtable写入的操作handler
  class LEVELDB_EXPORT Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };

	// leveldb在初始化writebach时候，rep_会预分配12个字节。
  WriteBatch();

  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();

  // 定义把key和value插入到wal log的接口，这个是基于磁盘的操作
  void Put(const Slice& key, const Slice& value);


  // 把其他的writebach中的数据，最佳到当前writebatch中
  void Append(const WriteBatch& source);


 private:
  friend class WriteBatchInternal;
  // 每次writebatch写入的数据存储在这里，批量写入
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};
```
- WriteBatch::Put分析
```
// put操作后的rep_的格式为: |SequenceNumber|kv count|Key Type|key length|key|value length|value|
void WriteBatch::Put(const Slice& key, const Slice& value) {
//  WriteBatchInternal::Count(this) 初始化为0.每次操作都加1。每个writebach数据中都会有12个字节的头，前8个字节记录当前writebatch的sequence num,后4个字节记录此次batch中kv的个数，写入到rep_[8]~rep_[11]
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // 这里开始正式写入当前kv的操作类型，在rep_[12]写入kTypeValue,目前支持2种类型，第一种是删除，第二种是非删除。
  // enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };这里定义了leveldb的两种类型的操作
  // rep_在初始化时候预留了12个字节，这个时候再次写入是在writebach header之后开始写入此次操作的类型
  rep_.push_back(static_cast<char>(kTypeValue));
  // 编码用户输入的key，格式为: key的长度|key的值
  PutLengthPrefixedSlice(&rep_, key);
  // 编码用户输入的value，格式为: value的长度|value的值
  PutLengthPrefixedSlice(&rep_, value);
}

// 把当前value的长度进行编码后写入到dst中，PutLengthPrefixedSlice中都是采用dst-Append的方式把长度和内容追加到writebach.rep_尾部
// 当前执行完PutLengthPrefixedSlice，value的格式为：{1个字节的操作类型}{4个字节编码的长度}{value的数据}
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  // 获取value的长度进行编码，使用了4个字节进行编码value的长度,然后把长度append到dst中
  PutVarint32(dst, value.size());
  // 把value的数据在append到dst的末尾
  dst->append(value.data(), value.size());
}

```
- DBImpl::Write分析
```
// DBImpl::Write保留部分讲解的实现
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  // May temporarily unlock and wait.
  // 初始化wal log文件、memtable、同时判断是否要做compaction
  Status status = MakeRoomForWrite(updates == nullptr);
  // 初始化versions_->LastSequence是从0开始，获取sequence num
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    // 把每一个writebach对应一个writer，然后放到writers_队列中，BuildBatchGroup函数则是把writers_队列中的writebatch合并到第一个writebatch中，同时更新第一个writebatch中的count
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    // 设置当前sequence num,当前writebatch中的前8个字节写入当前sequce num
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    // 下一个序列号等于当前序列号+当前writebatch中kv item的个数
    last_sequence += WriteBatchInternal::Count(write_batch);

    {
      mutex_.Unlock();
      // log_ 定义了wal日志的文件，这时候把当前writebatch中的rep_数据(可能包括多个wal log内容)，写入到当前的wal log文件中
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      // 根据oiptions中的同步策略来决定是否要刷盘
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      // 刷盘后需要把数据插入到memtable中
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();
    // 更新当前versions的sequence
    versions_->SetLastSequence(last_sequence);
  }

	// ----忽略-------
 
  return status;
}

```
- WriteBatchInternal::InsertInto分析,
```
// 这个函数主要是把writebatch中有固定格式的一个或者多个kv item，写入到memtable中
Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  // 根据writebatch中的rep_来解析此次batch中的sequence
  // batch中数据格式为: |序列号|kv item个数|key1类型|key1 size|key1 value|value1 size|value1|key2类型|key2 size|key2 value|value2 size|value2|
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  // 解析batch中的log数据，插入到memtable中
  return b->Iterate(&inserter);
}

// 定义memtable操作的handler
namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;
	// 解析出来的key和value插入到memtable，同时sequence+++
  void Put(const Slice& key, const Slice& value) override {
  	// 在skiplist实现的memtable中插入key和value
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
 // 解析出来的key,设置value为空串，插入到memtable来模拟删除操作，同时sequence+++
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

// 把writebatch.rep_中数据解析，按照操作类型一个kv一个kv的插入后插入到memtable中skiplist中，至此一个写入的完成流程完成
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  // 去除writebatch中的sequence num和kv item条数的header部分
  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    // 去除此次操作的kTypeValue
    char tag = input[0];
    // 然后去除这个字段的存储
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        // 解析key和value
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          // 然后插入到memtable中，操作完每次sequence++
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          // 这里也是一个插入操作，插入一个value为0 的key
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}
```