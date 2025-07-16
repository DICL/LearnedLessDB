// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "hyperleveldb/env.h"
#include "hyperleveldb/table.h"
#include "util/coding.h"
#include "util/coding.h"
#include "table/filter_block.h"
#include "table/block.h"
#include "koo/stats.h"
#include "koo/koo.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& /*key*/, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = LDBTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

#if BLEARN
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&), int level,
                       FileMetaData* meta, uint64_t lower, uint64_t upper,
                       bool learned, Version* version,
                       koo::LearnedIndexData** model, bool* file_learned) {
  Cache::Handle* handle = NULL;
  koo::Stats* instance = koo::Stats::GetInstance();

#if TIME_R || TIME_R_LEVEL
	std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
#if BOURBON_PLUS && REMOVE_MUTEX
	koo::LearnedIndexData* model_ = koo::file_data->GetModelForLookup(meta->number);
	if (model_ != nullptr) {
		*model = model_;
#else
  	*model = koo::file_data->GetModel(meta->number);
  	assert(file_learned != nullptr);
#endif
  	*file_learned = (*model)->Learned();

  	if (learned || *file_learned) {
  		LevelRead(options, file_number, file_size, k, arg, handle_result, level,
								meta, lower, upper, learned, version);
#if TIME_R || TIME_R_LEVEL
			std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
#if TIME_R
			koo::m_path += nano.count();
			koo::num_m_path++;
#endif
#if TIME_R_LEVEL
			koo::m_path_l[level] += nano.count();
			koo::num_m_path_l[level]++;
#endif
#endif
			return Status::OK();
		}
#if BOURBON_PLUS && REMOVE_MUTEX
	}
#endif

  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result, level, meta, lower, upper, learned, version);
    cache_->Release(handle);
  }
#if TIME_R || TIME_R_LEVEL
	std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
#if TIME_R
	koo::i_path += nano.count();
	koo::num_i_path++;
#endif
#if TIME_R_LEVEL
	koo::i_path_l[level] += nano.count();
	koo::num_i_path_l[level]++;
#endif
#endif
  return s;
}
#else
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
#if LEARN
	// Check if model exists
	koo::LearnedIndexData* model = koo::file_data->GetModel(file_number);
	if (model->Learned()) {
		return ModelGet(file_number, file_size, k, arg, saver, model);
	}
#endif
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}
#endif

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

#if LEARN && BLEARN
void TableCache::LevelRead(const ReadOptions& options, uint64_t file_number, 
														uint64_t file_size, const Slice& k, void* arg, 
														void (*handle_result)(void*, const Slice&, const Slice&), int level,
														FileMetaData* meta, uint64_t lower, uint64_t upper,
														bool learned, Version* version) {
	koo::Stats* instance = koo::Stats::GetInstance();

	// Find table
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(cache_->Value(handle));
  RandomAccessFile* file = tf->file;
  FilterBlockReader* filter = tf->table->rep_->filter;

	if (!learned) {
		ParsedInternalKey parsed_key;
	  ParseInternalKey(k, &parsed_key);
	  koo::LearnedIndexData* model = koo::file_data->GetModel(meta->number);
		auto bounds = model->GetPosition(parsed_key.user_key);
		lower = bounds.first;
	  upper = bounds.second;
		if (lower > model->MaxPosition()) {
			cache_->Release(handle);
			return;
		}
	}

  // Get the position we want to read
  // Get the data block index
  size_t index_lower = lower / koo::block_num_entries;
  size_t index_upper = upper / koo::block_num_entries;

  // if the given interval overlaps two data block, consult the index block to get
  // the largest key in the first data block and compare it with the target key
  // to decide which data block the key is in
  uint64_t i = index_lower;
  if (index_lower != index_upper) {
    Block* index_block = tf->table->rep_->index_block;
    uint32_t mid_index_entry = DecodeFixed32(index_block->data_ + index_block->restart_offset_ + index_lower * sizeof(uint32_t));
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = DecodeEntry(index_block->data_ + mid_index_entry,
                                      index_block->data_ + index_block->restart_offset_, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Index Entry Corruption");
    Slice mid_key(key_ptr, non_shared);
    int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
    i = comp < 0 ? index_upper : index_lower;
  }

  // Check Filter Block
  uint64_t block_offset = i * koo::block_size;
  if (filter != nullptr && !filter->KeyMayMatch(block_offset, k)) {
    cache_->Release(handle);
    return;
  }

  // Get the interval within the data block that the target key may lie in
  size_t pos_block_lower = i == index_lower ? lower % koo::block_num_entries : 0;
  size_t pos_block_upper = i == index_upper ? upper % koo::block_num_entries : koo::block_num_entries - 1;
#if DEBUG
  /*std::cout << lower << " " << upper << " " << koo::block_num_entries << std::endl;
  std::cout << index_lower << " " << index_upper << std::endl;
  std::cout << koo::block_size << " " << i << " " << block_offset << std::endl;
  std::cout << pos_block_lower << " " << pos_block_upper << std::endl;
  std::cout << std::endl;*/
#endif

  // Read corresponding entries
  size_t read_size = (pos_block_upper - pos_block_lower + 1) * koo::entry_size;
  static char scratch[4096];
  Slice entries;
  s = file->Read(block_offset + pos_block_lower * koo::entry_size, read_size, &entries, scratch);
  assert(s.ok());

  // Binary Search within the interval
  uint64_t left = pos_block_lower, right = pos_block_upper;
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = DecodeEntry(entries.data() + (mid - pos_block_lower) * koo::entry_size,
            entries.data() + read_size, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
    Slice mid_key(key_ptr, non_shared);
    int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
    if (comp < 0) left = mid + 1;
    else right = mid;
  }

  // decode the target entry to get the key and value (actually value_addr)
  uint32_t shared, non_shared, value_length;
  const char* key_ptr = DecodeEntry(entries.data() + (left - pos_block_lower) * koo::entry_size,
          entries.data() + read_size, &shared, &non_shared, &value_length);
  assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
  Slice key(key_ptr, non_shared), value(key_ptr + non_shared, value_length);
  handle_result(arg, key, value);

	cache_->Release(handle);
#if LOOKUP_ACCURACY
	size_t pos_block_mid, diff_abs;
	if (index_lower == index_upper) {
		pos_block_mid = (pos_block_lower + pos_block_upper) / 2 + 1;
		if (pos_block_mid > left) {
			diff_abs = pos_block_mid - left;
		} else {
			diff_abs = left - pos_block_mid;
		}
	} else {
		size_t mid = (lower + upper) / 2;
		size_t index_mid = mid / koo::block_num_entries;
		pos_block_mid = mid % koo::block_num_entries;
		if (pos_block_mid > left) {
			if (index_mid == i) diff_abs = pos_block_mid - left;
			else diff_abs = (koo::block_num_entries - pos_block_mid) + left;
		} else {
			if (index_mid == i) diff_abs = left - pos_block_mid;
			else diff_abs = (koo::block_num_entries - left) + pos_block_mid;
		}
	}
	if (diff_abs > 9) {			// 9까지는 가능. lower, upper 구할 때 각각 floor, ceil해서
		fprintf(stderr, "Error! diff_abs: %lu, level: %d, pos_mid: %lu, left: %lu, pos_lower: %lu, pos_upper: %lu\n\tindex_lower: %lu, index_upper: %lu, lower: %lu, upper: %lu\n", diff_abs, level, pos_block_mid, left, pos_block_lower, pos_block_upper, index_lower, index_upper, lower, upper);
	} else {
		koo::lm_num_error[level]++;
		koo::lm_error[level] += diff_abs;
		std::ofstream ofs("/koo/HyperBourbon/koo/data/lookup_error_"+std::to_string(level)+".txt", std::ios::app);
		ofs << diff_abs << std::endl;
		ofs.close();
	}
#endif
}

bool TableCache::FillData(const ReadOptions& options, FileMetaData* meta, koo::LearnedIndexData* data) {
	Cache::Handle* handle = nullptr;
	Status s = FindTable(meta->number, meta->file_size, &handle);

	if (s.ok()) {
		Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
		table->FillData(options, data);
		cache_->Release(handle);
		return true;
	} else return false;
}

#if MODEL_ACCURACY
void TableCache::TestModelAccuracy(uint64_t& file_number, uint64_t& file_size) {
	Cache::Handle* handle = nullptr;
	Status s = FindTable(file_number, file_size, &handle);
	if (s.ok()) {
		Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
		t->TestModelAccuracy(file_number);
	}
}
#endif
#endif

}  // namespace leveldb
