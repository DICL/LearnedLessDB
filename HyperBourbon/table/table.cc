// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "hyperleveldb/table.h"

#include "hyperleveldb/cache.h"
#include "hyperleveldb/comparator.h"
#include "hyperleveldb/env.h"
#include "hyperleveldb/filter_policy.h"
#include "hyperleveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "db/version_set.h"
#include "koo/koo.h"

#if LEARN
namespace koo { class LearnedIndexData; }
#endif

namespace leveldb {

#if LEARN
Table::Rep::~Rep() {
  delete filter;
  delete [] filter_data;
  delete index_block;
}
#else
struct Table::Rep {
  Rep()
    : options(),
      status(),
      file(NULL),
      cache_id(),
      filter(),
      filter_data(),
      metaindex_handle(),
      index_block() {
  }
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;

 private:
  Rep(const Rep&);
  Rep& operator = (const Rep&);
};
#endif

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    s = ReadBlock(file, ReadOptions(), footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* /*ignored*/) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& /*key*/, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

#if BLEARN
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&), int level,
                          FileMetaData* meta, uint64_t lower, uint64_t upper,
                          bool learned, Version* version) {
#else
Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
#endif
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
#if BLEARN
        (*handle_result)(arg, block_iter->key(), block_iter->value());
#else
        (*saver)(arg, block_iter->key(), block_iter->value());
#endif
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

#if LEARN
void Table::FillData(const ReadOptions& options, koo::LearnedIndexData* data) {
	if (data->filled) return;
  Status status;
  Block::Iter* index_iter = dynamic_cast<Block::Iter*>(rep_->index_block->NewIterator(rep_->options.comparator));
  for (uint32_t i = 0; i < index_iter->num_restarts_; ++i) {
    index_iter->SeekToRestartPoint(i);
    index_iter->ParseNextKey();
    assert(index_iter->Valid());
    Block::Iter* block_iter = dynamic_cast<Block::Iter*>(BlockReader(this, options, index_iter->value()));

    ParsedInternalKey parsed_key;
    int num_entries_this_block = 0;
    for (block_iter->SeekToRestartPoint(0); block_iter->ParseNextKey(); ++num_entries_this_block) {
        ParseInternalKey(block_iter->key(), &parsed_key);
#if YCSB_CXX
				data->string_keys.emplace_back(parsed_key.user_key.ToString());			// TODO 필요?
#else
        data->string_keys.emplace_back(parsed_key.user_key.data(), parsed_key.user_key.size());
#endif
    }

    if (!koo::block_num_entries_recorded) {
        koo::block_num_entries = num_entries_this_block;
        koo::block_num_entries_recorded = true;
        koo::entry_size = block_iter->restarts_ / num_entries_this_block;
        BlockHandle temp;
        Slice temp_slice = index_iter->value();
        temp.DecodeFrom(&temp_slice);
        koo::block_size = temp.size() + kBlockTrailerSize;
    }
    delete block_iter;
  }
  data->filled = true;
  delete index_iter;
}

#if MODEL_ACCURACY
void Table::TestModelAccuracy(uint64_t& file_number) {
	std::vector<uint64_t> keys;

	Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
	iiter->SeekToFirst();
	while (iiter->Valid()) {
		Slice handle_value = iiter->value();
		BlockHandle handle;

		if (handle.DecodeFrom(&handle_value).ok()) {
			Iterator* block_iter = BlockReader(this, koo::read_options, iiter->value());
			block_iter->SeekToFirst();
			ParsedInternalKey parsed_key;
			while (block_iter->Valid()) {
				ParseInternalKey(block_iter->key(), &parsed_key);
				keys.emplace_back(koo::SliceToInteger(parsed_key.user_key));
				//std::string string_key(parsed_key.user_key.data(), parsed_key.user_key.size());
				block_iter->Next();
			}
		} else std::cout << "Decode error" << std::endl;

		iiter->Next();
	}
#if DEBUG
	// keys
	std::ofstream of_keys;
	of_keys.open("/koo/HyperLevelDB/koo/data/keys_" + std::to_string(file_number) + ".txt");
	int cnt = 0;
	for (auto& key : keys) {
		std::string str = "(" + std::to_string(key) + ", " + std::to_string(cnt++) + ")\n";
		of_keys.write(str.c_str(), str.size());
	}
	of_keys.close();
#endif

	// Accuracy test
	koo::LearnedIndexData* model = koo::file_data->GetModelForLookup(file_number);
	if (!model->Learned()) return;
	std::vector<koo::Segment> &segs = model->string_segments;
	uint64_t keys_size = model->size;
	if (keys_size != keys.size()) { std::cout << "ERROR keys_size != keys.size()" << std::endl; return; }

/*	double diff_max = 0.0, diff_sum = 0.0;
	int start = 0;
	uint64_t end;
	int segs_size = segs.size();
	int max_idx = -1;
	std::string str_diff = "";
	int cnt_overmax = 0;
	int cnt_keys = 0;
	int error_bound = LEARN_MODEL_ERROR;
	for (int i=0; i<segs_size-1; i++) {			// dummy segment
		koo::Segment seg = segs[i];
		end = seg.x_last;
		//if (i == segs_size-1 && end < keys_size-1) end = keys_size-1;
		str_diff += "\n" + std::to_string(i)+ ": (" + std::to_string(seg.x) + ", ) ~ (" + std::to_string(seg.x_last) + ", " + std::to_string(seg.y_last) + "): y = " + std::to_string(seg.k) + " * x + " + std::to_string(seg.b) + "\n";
		for (int j=start; j<keys_size; j++) {
			double x_real = keys[j];
			if (x_real > end) { start = j; break; }
			double y_real = j;
			double y_inf = seg.k * x_real + seg.b;

			double diff = fabs(y_real - y_inf);
			if (diff > error_bound) cnt_overmax++;
			str_diff += "(" + std::to_string(x_real) + ", " + std::to_string(y_real) + ")\t" + std::to_string(y_inf)
								+ "\t\tDiff = " + std::to_string(y_real - y_inf) + "\n";
			diff_sum += diff;
			if (diff > diff_max) {
				diff_max = diff;
				max_idx = j;
			}
			cnt_keys++;
		}
	}

	double diff_avg = diff_sum / keys_size;
#if DEBUG || AC_TEST
	if (diff_max > error_bound) {
		std::cout << std::endl;
		std::cout << "******************* Error Bound **********************" << std::endl;
		std::cout << "File number: " << file_number << ", level = " << model->level << ", # keys: " << keys_size << ", min_key: " << keys.front() << ", max_key: " << keys.back() << std::endl;
		std::cout << "# segments: " << segs_size << ", min_x: " << segs.front().x << ", max_x_last: " << segs[segs_size-2].x_last << std::endl;
		std::cout << "Average error bound: +-" << diff_avg << std::endl;
		std::cout << "Maximum error bound: +-" << diff_max << std::endl;
		std::cout << "Max index: " << max_idx << std::endl;
		std::cout << "# keys whose diff > error bound: " << cnt_overmax << std::endl;
		if (keys_size != cnt_keys) std::cout << "!!!! Model does not cover all keys !!!!" << std::endl;
		std::cout << "******************************************************" << std::endl;
		std::cout << std::endl;
	}
	std::ofstream of_diff;
	of_diff.open("/koo/HyperLevelDB/koo/data/diff_" + std::to_string(file_number) + ".txt");
	of_diff << "min_key: " << std::to_string(keys.front()) << ", max_key: " << std::to_string(keys.back()) << "\n";
	of_diff << "Average error bound: +-" << diff_avg << "\n";
	of_diff << "Maximum error bound: +-" << diff_max << "\n";
	of_diff << "Max index: " << max_idx << "\n\n";
	of_diff.write(str_diff.c_str(), str_diff.size());
	of_diff.close();
#endif
*/	
	// For emulation
	/*std::ofstream of_keys_;
	of_keys.open("/koo/merging-plr/real_HyperLevelDB_keys/test/cutting/keys_" + std::to_string(file_number) + ".txt");
	for (auto& key : keys) of_keys << key << "\n";
	of_keys_.close();*/

	// Accuracy test
	bool skip = false;
	if (keys_size < 2 || keys.size() < 2 || model->min_key == model->max_key || keys.front() == keys.back()) {
		skip = true;
		fprintf(stderr, "ERROR fn: %lu\n", model->file_number);
	}

	double diff_max = 0.0, diff_sum = 0.0;
	int start = 0;
	uint64_t end;
	int segs_size = segs.size();
	int max_idx = -1;
	int cnt_overmax = 0;
	int cnt_keys = 0;
	int error_bound = model->GetError();
	for (int i=0; i<segs_size-1; i++) {			// dummy segment
		koo::Segment seg = segs[i];
		if (i == segs_size-2) end = model->max_key;
		else end = segs[i+1].x - 1;
		//if (i == segs_size-1 && end < keys_size-1) end = keys_size-1;
		for (uint64_t j=start; j<keys_size; j++) {
			uint64_t key = keys[j];
			if (key > end) { start = j; break; }
			double x_real = static_cast<double>(key);
			uint64_t y_real = j;
			double y_inf = seg.k * x_real + seg.b;

			double diff = fabs(y_real - y_inf);
			if (diff > error_bound) cnt_overmax++;
			diff_sum += diff;
			if (diff > diff_max) {
				diff_max = diff;
				max_idx = j;
			}
			cnt_keys++;
		}
	}

	if (cnt_keys != keys_size) { fprintf(stderr, "ERROR!! not match fn: %lu\n", model->file_number); }

	double diff_avg = diff_sum / keys_size;

	uint64_t diff_avg_uint = static_cast<uint64_t>(std::round(diff_avg));
	uint64_t diff_max_uint = static_cast<uint64_t>(std::round(diff_max));
	if (std::isnan(diff_avg_uint) || std::isinf(diff_avg_uint)) {
		fprintf(stderr, "ERROR!!! fn: %lu, %lu\n", model->file_number, diff_avg_uint);
		skip = true;
	}
	if (!skip) {
		koo::lm_num_error[model->level]++;
		koo::lm_avg_error[model->level] += diff_avg_uint;
		koo::lm_max_error[model->level] += diff_max_uint;

		// Error 표준편차 
		std::ofstream of_stdev;
		of_stdev.open("/koo/HyperBourbon/koo/data/error_avg_stdev_level"+std::to_string(model->level)+".txt", std::ios::app);
		of_stdev << diff_avg << "\n";
		of_stdev.close();
		of_stdev.open("/koo/HyperBourbon/koo/data/error_max_stdev_level"+std::to_string(model->level)+".txt", std::ios::app);
		of_stdev << diff_max << "\n";
		of_stdev.close();
	}
//#if DEBUG && AC_TEST
}
#endif
#endif

}  // namespace leveldb
