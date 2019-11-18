// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

//kFilterBase：2KB
//该参数的本意为：如果多个key-value的总长度超过了2KB，就应该计算key的位图，
//但是发起计算Bloom filter的时机是由TableBuilder::Flush函数
//因此并非2KB的数据就会发起一轮Bloom filter的计算，比如block_size等于7KB，
//可能造成多轮的GenerateFilter函数调用，而除了第一轮的调用会产生位图，
//其他两轮相当于轮空，只是将result_的size再次放入filter_offesets_

//考虑下面的情况：
//如果block_size是7KB，那么（0~7KB-1）是属于第一个data_block
//(7KB ~ 14KB-1)属于第二个data_block
//(0~6k-1)没有什么问题，但是（6k~7k-1）在存放bloom filter时，指向的是第二个bloom filter，将可能带来问题。
//实际上不会，因为meta只是data block的辅助，应用层绝不会问data block为6K的block位图在何方。
//从查找的途径来看，先根据key的index block，找到对应的data_block，而data_block的offset只是0或者7K，绝不会是6K.

//当传入data_block的offset是7K的时候，根据上表，就会返回第二个bloom filter，而第二个bloom filter会负责整个第二个data block的全部key
//即data_block的（7k~14k-1）范围内的所有key，都可以利用第二个bloom filter找到。

//按照上面的分析，如果block_offset为6K的时候，就会找不到对应位图，因为start和limit指向的都是bitmap 1，函数的filter最终为空，
//但是没关系，绝不会传进来6k，因为6k不是两个data_block的边界
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);

  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  //如果filter中key个数为0，直接压入result_.size()，然后返回，（result为计算出来的位图）
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);

  //得到本轮的所有的keys，放入tmp_keys_数组
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  //为当前key集合产生filter，并append到result_中。
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  //清空，重置
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
