// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
//注意：因为sstable中的key个数很多，当攒了足够的key就会计算一批位图，再攒一批key，又计算一批位图
//那么这么多bloom filter的位图，必须要分割开，否则就混了。

//这里的FilterBlock就是sstable中的meta block，位于data block之后。
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  //开始构建新的filter block，TableBuilder在构造函数和Flush函数中调用
  void StartBlock(uint64_t block_offset);
  //添加key，TableBuilder每次向data block中加入key时调用
  void AddKey(const Slice& key);
  //结束构建，TableBuilder在结束对table的构建时调用
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;   //filter类型
  std::string keys_;             // Flattened key contents，暂时存放本轮所有的key，追加往后写入
  std::vector<size_t> start_;    // Starting index in keys_ of each key，记录本轮key与key之间的边界的位置，便于分割成多个key
  std::string result_;           // Filter data computed so far，计算出来的位图，多轮计算往后追加写入
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument，将本轮的所有key，存入该vector，其实并无存在的必要，用临时变量即可
  std::vector<uint32_t> filter_offsets_;  //计算出来的多个位图的边界位置，用于分割多轮keys产生的位图
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
