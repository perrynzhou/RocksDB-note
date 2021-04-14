// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

// 把writebatch中数据解析后插入到memtable中
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

// WriteBatch.rep_是WriteBatch构造函数时候初始化了12个字节，12个字节的最后4个字节用于存储kv个数
// WriteBatchInternal::Count是读取当前WriteBatch中kv个个数
int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}
// 每次WriteBatch写入kv时候，都会先读取之前的WriteBatch的count,在加n,最后写入rep_的开始12个字节的的最后4个字节
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  // 8~12个rep_位置写入了此次batch的kv个数
  EncodeFixed32(&b->rep_[8], n);
}

// 设置SequenceNumbe,写入rep_的开始12个字节的8个字节，解析这8个字节为sequence num
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

// put操作后的rep_的格式为: |SequenceNumber|kv count|Key Type|key length|key|value length|value|
void WriteBatch::Put(const Slice& key, const Slice& value) {
//  WriteBatchInternal::Count(this) + 1 返回是一个32位的整数，记录此次batch中kv的个数，每次都累加,写入到rep_[8]开始的后面4位
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // rep_[0]写入kTypeValue,目前支持2种类型，第一种是删除，第二种是非删除,这个数据是从12个字节之后写入
  rep_.push_back(static_cast<char>(kTypeValue));
  // 编码用户输入的key，格式为: key的长度|key的值
  PutLengthPrefixedSlice(&rep_, key);
  // 编码用户输入的value，格式为: value的长度|value的值
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  // 根据writebatch中的rep_来解析此次batch中的sequence
  // batch中数据格式为: |序列号|kv item个数|key1类型|key1 size|key1 value|value1 size|value1|key2类型|key2 size|key2 value|value2 size|value2|
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  // 解析batch中的log数据，插入到memtable中
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

// 这里把src中的wal log的格式追加到dst的wal log中，组织成一个字符串格式,同时设置writebatch中wal log的kv item的个数
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
