// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;           //引用计数
  int allowed_seeks;  // Seeks allowed until compaction  //compact之前允许的seek次数
  uint64_t number;       //FileNumber（文件名）
  uint64_t file_size;    // File size in bytes （文件的大小）
  InternalKey smallest;  // Smallest internal key served by table（sstable中最小的key）
  InternalKey largest;   // Largest internal key served by table （sstable中最大的key）
};

//LevelDB中对Manifest的Decode/Encode是通过类VersionEdit完成，Manifest文件保存了leveldb的管理元信息
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  //清空信息
  void Clear();

  //Set函数，设置信息
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }

  //{level，key}指定的compact点加入到compact_pointers_
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  //添加sstable文件信息
  //level：sst文件层次
  //file：文件编号-用作文件名
  //size：文件大小
  //smallest，largest：sst文件包含k/v对的最大最小key
  //根据参数生产一个FileMetaData对象，把sstable文件信息添加到new_files_数组中
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    printf("AddFile()!\n");
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  //从指定level删除文件
  //把参数指定的文件添加到deleted_files_
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  //将信息encode到一个string中
  void EncodeTo(std::string* dst) const;
  //从Slice中Decode出DB元信息
  Status DecodeFrom(const Slice& src);

  
  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;    //key compactor名字
  uint64_t log_number_;       //日志编号
  uint64_t prev_log_number_;  //前一个日志编号
  uint64_t next_file_number_;  //下一个文件编号
  SequenceNumber last_sequence_; //上一个seq
  bool has_comparator_;          //是否有compactor
  bool has_log_number_;          //是否有log_number
  bool has_prev_log_number_;     //是否有pre_log_number
  bool has_next_file_number_;    //是否有next_file_number
  bool has_last_sequence_;       //是否有last_sequence

  std::vector<std::pair<int, InternalKey>> compact_pointers_;     //compact点
  DeletedFileSet deleted_files_;                                  //删除文件集合
  std::vector<std::pair<int, FileMetaData>> new_files_;  //int代表level，新文件集合
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
