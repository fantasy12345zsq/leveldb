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

Status WriteBatch::Iterate(Handler* handler) const {

  // printf("WriteBatch::Iterate()!\n");

  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  // for(int i = 0; i < input.size(); i++)
  // {
    // printf(" %d ",input[i]);
  // }
  // printf("\n");

  input.remove_prefix(kHeader);
  
  // for(int i = 0; i < input.size(); i++)
  // {
    // printf(" %d ",input[i]);
  // }
  // printf("\n");
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
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

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  // printf("WriteBatchInternal::SetCount!\n");
  // printf("n = %d\n",n);

  //b中含有一个std::string rep_的成员函数
  //&b->rep_[8]代表，rep_[8]的地址，也就是字符串rep_第8个位置的地址用来保存n
  EncodeFixed32(&b->rep_[8], n);
  // printf("%u\n",b->rep_[8]);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  // printf("WriteBatch::Put()!\n");
  // printf("key.data = %s,value.data = %s\n",key.data(),value.data());

  //count函数计算当前的writebatch中有多少对key-value
  //setCount将当前的键值对数加1，因为这里新加入了一对键值
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  // printf("rep_ = %ld\n",rep_.size());

  //将键值对的type加入到rep_末尾
  rep_.push_back(static_cast<char>(kTypeValue));
  // printf("rep_ = %ld\n",rep_.size());
  // for(int i = 0 ; i < rep_.size(); i++)
  // {
  //   printf("%u\n",rep_[i]);
  // // }

  // printf("key!\n");

  //将键值对（包括长度加入）加入到rep_中
  PutLengthPrefixedSlice(&rep_, key);
  // for(int i = 0 ; i < rep_.size(); i++)
  // {
    // printf("%u\n",rep_[i]);
  // }
  // printf("value!\n");
  PutLengthPrefixedSlice(&rep_, value);
  // for(int i = 0 ; i < rep_.size(); i++)
  // {
    // printf("%u\n",rep_[i]);
  // }
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
    // printf("MemTableInserter::Put()!\n");
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

  // printf("WriteBatchIntrenal::InsertInto()!\n");
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
