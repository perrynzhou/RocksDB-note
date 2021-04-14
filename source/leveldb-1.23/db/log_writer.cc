// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;
// 当前writebatch中的log以record形式写入到wal log
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    // 默认的block大小是kBlockSize=32K，初始化的时候block_offset_为0
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // 当block剩余的空间小于,block header大小7个字节时候会执行
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        // 直接在尾部填充0
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // 重置当前block使用位置，下一次就从新的block开始写入，block_offset_=0
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    // 确保32K-当前block已经写入的位置- block头的7个字节必须大于等于0
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 获取当前可写入的block的大小
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 如果当前batch中的一个或者多个kv log大小和当前block的大小比较，确定当前是全部能写入wal log中的block、部分写入等
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // end 是代表left大小和fragment_length一直，则表示全部写入kFullType，一个block被写满
    const bool end = (left == fragment_length);
    if (begin && end) {
      // batch中的wal log可以全部写入到block中
      type = kFullType;
    } else if (begin) {
      // 这里batch的数据开头的一部分写入
      type = kFirstType;
    } else if (end) {
      // 这里batch的数据最后一部分写入
      type = kLastType;
    } else {
      type = kMiddleType;
    }
    // writebatch中的数据写入到wal 组织的block中
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

// writebatch中数据持久化到wal组织的block中
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // wal 日志的header,占用7个字节Format the header
  char buf[kHeaderSize];
  // header的前面三个分别写入长度的高8位、低8位、写入block类型
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // 计算当前写入数据的crc校验码，占用4个字节，持久化到block中
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 当前写入到block中的格式位： |checksum|length low 8 byte|length high 8 byte|type|data|
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
