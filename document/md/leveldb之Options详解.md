## leveldb之Options详解

| 作者 | 时间 |QQ技术交流群 |
| ------ | ------ |------ |
| perrynzhou@gmail.com |2020/12/01 |672152841 |

#### leveldb中Options有什么作用？

- Options 定义了打开leveldb时候的行为包括了key的比较函数、整个数据库的读写读写方式、后台任务、全局的日志、Memtable的上限、数据库打开文件的最大个数、Cache初始化、block的大小、默认压缩方式、基于磁盘读的过滤等，Options 定义了整个数据库打开的参数的入口。

- leveldb同时也定义了ReadOptions和WriteIOptions分别来定义leveldb读和写的参数控制

#### Options定义是什么？都有哪些Options选型？

- 打开数据库全局参数Options定义
```
// 打开数据库时候传入，来确定打开leveldb时候的数据库的参数
struct LEVELDB_EXPORT Options {
	// 默认参数传入
	Options();
  // 每个key在sstable中的排序函数，默认是按照字节比较的
  const Comparator* comparator;

  // 打开数据库时候如果数据库目录不存在，来控制是创建还是退出
  bool create_if_missing = false;

  // If true, an error is raised if the database already exists.
  // 
  bool error_if_exists = false;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any
  // errors.  This may have unforeseen ramifications: for example, a
  // corruption of one DB entry may cause a large number of entries to
  // become unreadable or for the entire DB to become unopenable.
  bool paranoid_checks = false;

  
  // 封装每个文件读写、调度后台任务等
  Env* env;

  // 基于文件的log
  Logger* info_log = nullptr;

  // 每个active memtable的内存消耗的上限，一旦超过转换为不可更改的memtable
  size_t write_buffer_size = 4 * 1024 * 1024;


  // 每个leveldb的数据库打开的最大文件数
  int max_open_files = 1000;
  
  // 磁盘block的Cache定义和初始化
  Cache* block_cache = nullptr;

  // leveldb中每个block大小
  size_t block_size = 4 * 1024;

  
  int block_restart_interval = 16;

  // 每个文件写入的数据量，当超过会切换为新的一个文件
  size_t max_file_size = 2 * 1024 * 1024;
  // 压缩方式
  CompressionType compression = kSnappyCompression;


  bool reuse_logs = false;


  // 通过过滤器来判断数据是否在磁盘上，这里可以使用布隆过滤器
  const FilterPolicy* filter_policy = nullptr;
};

// 全局Options构造函数
// BytewiseComparator定义了两个字符串的比较函数
// Env 是全局虚函数，分别定义了PosixEnv和WindowsEnv，分别代表Linux/Unix和Windows下针对leveldb的文件操作的实现
Options::Options() : comparator(BytewiseComparator()), env(Env::Default()) {}

```
- 数据库读操作ReadOptions参数控制定义
```
// 控制读操作的参数
struct LEVELDB_EXPORT ReadOptions {
  ReadOptions() = default;

  // 来控制是否每次读都做数据的checksum
  bool verify_checksums = false;

  // 每次读操作后数据是否要缓存在内存中
  bool fill_cache = true;

  // snapshot指针初始化
  const Snapshot* snapshot = nullptr;
};
```
- 数据库写操作
```
/ 写操作控制参数定义
struct LEVELDB_EXPORT WriteOptions {
	// 默认参数初始化
  WriteOptions() = default;

  // 每次写操作是否要wal 日志刷盘的控制
  bool sync = false;
};

```

