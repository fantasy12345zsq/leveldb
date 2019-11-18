// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;     //data block选项
  Options index_block_options; //index block选项
  WritableFile* file;     //sstable文件
  uint64_t offset;        //要写入data block在sstable文件中的偏移，初始为0
  Status status;          //当前状态，初始ok
  BlockBuilder data_block; //当前操作的data block
  BlockBuilder index_block; //sstable的index block
  std::string last_key;     //当前data block最后的k/v对的key
  int64_t num_entries;      //当前data block的个数，初始为0
  bool closed;  // Either Finish() or Abandon() has been called.，调用了Finish()或Abandon()，初始为False
  FilterBlockBuilder* filter_block; //根据filter数据快速定位key是否在block中

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;  //见Add函数
  BlockHandle pending_handle;  // Handle to add to index block，添加到index block的data block的信息

  std::string compressed_output;  //压缩后的data block
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {

  // printf("TableBuilder::TableBuilder()!\n");
  if (rep_->filter_block != nullptr) {
    printf("rep_->filter_block != null!\n");
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {

  // printf("TableBuilder::Add()!\n");

  Rep* r = rep_;
  // printf("last_key: ");
    
  // for(int i = 0; i < r->last_key.size(); i++)
  // {
    // printf(" %d ",(r->last_key)[i]);
  // }

  //首先保证文件没有被close，也就是没有调用过Finish/Abandon
  assert(!r->closed);
  //保证当前状态是ok的  
  if (!ok()) return;
  //如果当前有缓存的k/v对，保证新加入的key是最大的
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  //pending_index_entry意义：
  //直到遇到下一个data block的第一个key时，我们才为上一个data block生成index entry
  //这样做的好处是：可以为index使用较短的key，
  //比如：上一个data block最后一个k/v是“the quick brown fox”，其后继data block的第一个key是“the who”
  //我们就可以用一个较短的字符串“the r”作为上一个data block的index block entry的key
  //简言之，就是开始下一个data block时，leveldb才将上一个data block加入到index block中。
  //标记pending_index_entry就是干这个的。
  //
  if (r->pending_index_entry) {
    // printf("r->pending_index_entry\n");
    assert(r->data_block.empty());
    // printf("last_key: ");
    
    // for(int i = 0; i < r->last_key.size(); i++)
    // {
      // printf(" %d ",(r->last_key)[i]);
    // }
    // printf("\n key: ");
    // for(int i = 0; i < key.size(); i++)
    // {
      // printf(" %d ", key[i]);
    // }
    // printf("\n");
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    // printf("index_block Add()!\n");
    // printf("last_key: ");
    // for(int i = 0; i < r->last_key.size(); i++)
    // {
      // printf(" %d ",(r->last_key)[i]);
    // }
    // printf("\n handle_endcodeing: ");
    // for(int i = 0; i < handle_encoding.size(); i++)
    // {
      // printf(" %d ",handle_encoding[i]);
// /    }
    // printf("\n");
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  //如果filter不空，就把key加入到filter_block中
  if (r->filter_block != nullptr) {
    // printf("r->filter_block != NULL!\n");
    r->filter_block->AddKey(key);
  }

  //设置last_key = key
  //将key，value添加到r->data_block中，更新entry数
  r->last_key.assign(key.data(), key.size());
  // printf("key.data = %s, value.data = %s\n",key.data(),value.data());
  r->num_entries++;
  // printf("r->num_entries = %d\n",r->num_entries);
  r->data_block.Add(key, value);

  //如果data block的个数超过限制，就立刻Flush到文件中
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  printf("estimate_block_size = %d, r->options.block_size = %d\n",estimated_block_size, r->options.block_size);
  if (estimated_block_size >= r->options.block_size) {
    // printf("Flush()!\n");
    Flush();
  }
}

void TableBuilder::Flush() {
  // printf("TableBuilder::Flush()!\n");
  Rep* r = rep_;
  //首先保证文件未关闭，并且状态ok
  assert(!r->closed);
  if (!ok()) return;
  //data block是空的
  if (r->data_block.empty()) return;
  
  //保证pending_index_entry是false，即data block的Add已经完成
  assert(!r->pending_index_entry);

  //写入data block，并设置其index entry信息（BlockHandle对象）
  WriteBlock(&r->data_block, &r->pending_handle);

  //写入成功，则Flush文件，并设置r->pending_index_entry为true
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }

  //将data block在sstable中的偏移加入到filter block中
  //并指明开始新的data block
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  // printf("TableBulder::WriteBlock()!\n");
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();
  // for(int i = 0; i < raw.size(); i++)
  // {
    // printf(" %d ", raw[i]);
  // }
  // printf("\n");

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  // printf("TableBuilder::WriteRawBlock()!\n");
  // for(int i = 0; i < block_contents.size(); i++)
  // {
    // printf(" %d ", block_contents[i]);
  // }
  // printf("\n");
  
  Rep* r = rep_;
  // printf("r->offset = %d\n", r->offset);
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];    //kBlockTrainerSize = 5
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
    // printf("trainer: ");
    // for(int i = 0; i < 5; i++)
    // {
      // printf(" %d ", trailer[i]);
    // }
    // printf("\n");
    // printf("r->offset = %d\n", r->offset);
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  printf("TableBuilder::Finish()!\n");
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }
    // printf("neta_index_block!\n");
    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      // printf("last_key: ");
      // for(int i = 0; i < r->last_key.size();i++)
      // {
        // printf(" %d ", (r->last_key)[i]);
      // }
      // printf("\n handle_codeing: ");
      for (int i = 0; i < handle_encoding.size();i++)
      {
        // printf(" %d ",handle_encoding[i]);
      }
      // printf("\n");
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    // printf("index_block!\n");
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    // printf("Write footer!\n");
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
