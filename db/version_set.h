// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {
 public:
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  //为该Version中的所有sstable都创建一个Two Level Iterator，以遍历sstable的内容
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  //如果指定level中的某些文件和[*smallest_user_key, *largest_user_key]有重合就返回true
  //smallest_user_key==NULL，表示比DB中所有key都小的key
  //largest_user_key==NULL,表示比DB中所有的key都大的key
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  //返回我们应该在哪个level上放置新的memtable compaction
  //该compaction覆盖了范围[smallest_user_key, largest_user_key]
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);
  //指定level的sstable个数
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  //可见一个version就是一个sstable文件的集合，以及它管理的compaction集合
  //Version通过Version* prev和*next指针构成了一个Version双向循环链表
  //表头指针则在VersionSet中

  VersionSet* vset_;  // VersionSet to which this Version belongs
  //链表指针
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  //引用计数
  int refs_;          // Number of live refs to this version

  // List of files per level
  //每个level的所有sstable元信息
  //files_[i]中的FileMetaData按照FileMetaData::Smallest排序
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;  //下一个要compact的文件（allow_seeks用光）
  int file_to_compact_level_;   

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  //下一个应该compact的level和compaction分数
  //分数<1说明compaction并不紧迫，这些字段在Finalize()中初始化
  double compaction_score_;
  int compaction_level_;
};

class VersionSet {
 public:
  //构造函数
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  //
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  //恢复函数，从磁盘恢复最后保存的元信息
  Status Recover(bool* save_manifest);

  // Return the current version.
  //返回current version
  Version* current() const { return current_; }

  // Return the current manifest file number
  //当前的MANIFEST文件号
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  //分配并返回新的文件编号
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  //
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  //返回指定level的文件个数
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  //返回指定level中所有sstable文件大小的和
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  //
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  //标记指定的文件编号已经被使用
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  //获取文件编号
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  //返回正在compact的log文件编号，如果没有返回0
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  //
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  //第一组：直接来自于DBImpl，构造函数传入
  Env* const env_;   //操作系统封装
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;

  //第二组：db元信息相关
  uint64_t next_file_number_;        //log文件编号
  uint64_t manifest_file_number_;    //manfiest文件编号
  uint64_t last_sequence_;
  uint64_t log_number_;              //log编号
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  //第三组：menifest文件相关
  WritableFile* descriptor_file_;   
  log::Writer* descriptor_log_;

  //第三组：版本管理
  Version dummy_versions_;  // Head of circular doubly-linked list of versions. //版本管理
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  //level下一次compaction的开始key， 空字符串或者合法的Internalkey
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
//LevelDB中Compaction从大的类别中分为两种：
//MinorCompaction，指的是immutable memtable持久化为sst文件
//Major Compaction，指的是sst文件之间的compaction

//Major Compaction分为三种：
//1）Manual Compaction，人工触发的Compaction，由外部接口调用产生。
//2）Size Compaction，根据每个level的总文件大小来触发
//         leveldb核心Compact过程，其主要是为了均衡各个level的数据，从而保证读写的性能均衡
//         leveldb会计算每个level的总的文件大小，并根据此计算score，最后根据score来选择合适的level和文件进行compact，具体计算方法如下：
//         对于level 0 ：文件个数阈值为4
//             score = level 0的文件总数 / 4
//         对于其他level，每个level所有文件的总大小的一个阈值
//         第0层： 10M
//         第1层： 10M
//         第2层： 100M
//         第3层： 1000M（1G）
//         第4层： 10000M（10G）
//         第5层： 100000M（100G）
//3）Seek Compaction，每个文件的seek miss次数都有一个阈值，如果超过了这个阈值，那么认为这个文件需要Compact
//Size Compaction优先级大于Seek Compaction
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;   //要compact的level
  uint64_t max_output_file_size_; //生成sstable最大的size
  Version* input_version_;        //compact时当前的Version
  VersionEdit edit_;              //记录compact过程的操作

  // Each compaction reads inputs from "level_" and "level_+1"
  //inputs_[0]为level n 的sstable文件信息
  //inputs_[1]为level n+1 的sstable文件信息
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  //位于level n+2，并且与compact的key-range有overlap的sstable
  //保存grandparents_是因为compact最终会产生一系列level n+1的sstable
  //而如果生成的sstable与level n+2中有过多的overlap的话，当compact
  //level n+1时会产生过多的merge，为了尽量避免这种情况，compact过程中
  //需要检查与level n+2中产生overlap的size并与
  //阈值kMaxGrandParentOverlapBytes作比较，
  //以便提前终止compact
  std::vector<FileMetaData*> grandparents_;
  
  //记录compact时grandparents_中已经overlap的index
  size_t grandparent_index_;  // Index in grandparent_starts_
  
  //
  bool seen_key_;             // Some output key has been seen
  
  //记录已经overlap_的累计size
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  //compact时，当key的ValueType是kTypeDeletion时，
  //要检查其在level n+1以上是否存在（IsBaseLevelForKey()）
  //来决定是否丢弃掉该key。因为compact时，key的遍历是顺序的
  //所以每次检查从上一次检查结束的地方开始执行
  //level_ptrs_[i]中就记录了input_version_->levels_[i]中，上一次比较结束的sstable下标
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
