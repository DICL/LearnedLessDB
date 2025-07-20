// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "hyperleveldb/env.h"
#include "hyperleveldb/table.h"
#include "util/coding.h"
#include "koo/koo.h"
#include "util/coding.h"
#include "table/filter_block.h"
#include "table/block.h"

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

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
#if RETRAIN || TIME_R_LEVEL || AC_TEST2 || LOOKUP_ACCURACY
											 int level, FileMetaData* meta,
#endif
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
#if TIME_R || TIME_R_LEVEL || AC_TEST2
	std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
	// Check if model exists
	koo::LearnedIndexData* model = koo::file_data->GetModelForLookup(file_number);
	if (model != nullptr && model->Learned()) {
#if RETRAIN || LOOKUP_ACCURACY
		Status s = ModelGet(file_number, file_size, k, arg, saver, level, meta, model);
#else
		Status s = ModelGet(file_number, file_size, k, arg, saver, model);
#endif
#if TIME_R || TIME_R_LEVEL || AC_TEST2
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		if (model->Merged()) {
#if TIME_R
			koo::m_path += nano.count();
			koo::num_m_path++;
#endif
#if TIME_R_LEVEL
			koo::m_path_l[level] += nano.count();
			koo::num_m_path_l[level]++;
#endif
#if AC_TEST2
			koo::served_m_time[level-2].val += nano.count();
			koo::served_m[level-2].val++;
#endif
		} else {
#if TIME_R
			koo::l_path += nano.count();
			koo::num_l_path++;
#endif
#if TIME_R_LEVEL
			koo::l_path_l[level] += nano.count();
			koo::num_l_path_l[level]++;
#endif
#if AC_TEST2
#if RETRAIN
			if (model->is_retrained_) {
				if (level == 0) fprintf(stderr, "!!!!!!!L0\n");
				koo::served_r_time[level-2].val += nano.count();
				koo::served_r[level-2].val++;
			} else {
#endif
				koo::served_l_time[level].val += nano.count();
				koo::served_l[level].val++;
#if RETRAIN
			}
#endif
#endif
		}
#endif
		return s;
	}
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
#if TIME_R || TIME_R_LEVEL || AC_TEST2
	std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
#if TIME_R
	koo::i_path += nano.count();
	koo::num_i_path++;
#endif
#if TIME_R_LEVEL
	koo::i_path_l[level] += nano.count();
	koo::num_i_path_l[level]++;
#endif
#if AC_TEST2
	koo::served_i_time[level].val += nano.count();
	koo::served_i[level].val++;
#endif
#endif
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

Status TableCache::ModelGet(uint64_t file_number, uint64_t file_size, const Slice& k, 
														void* arg, void (*saver)(void*, const Slice&, const Slice&),
#if RETRAIN || LOOKUP_ACCURACY
													  int level, FileMetaData* meta,
#endif
														koo::LearnedIndexData* model) {
	// Find table
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(cache_->Value(handle));

	// Search the segment
  ParsedInternalKey parsed_key;
  ParseInternalKey(k, &parsed_key);
  auto bounds = model->GetPosition(parsed_key.user_key);
  uint64_t lower = bounds.first;
  uint64_t upper = bounds.second;
  if (lower > model->MaxPosition()) {
  	cache_->Release(handle);
  	return s;
	}
	bool isMergedModel = model->Merged();

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
  FilterBlockReader* filter = tf->table->rep_->filter;
  if (filter != nullptr && !filter->KeyMayMatch(block_offset, k)) {
    cache_->Release(handle);
    return s;
  }

  // Get the interval within the data block that the target key may lie in
  size_t pos_block_lower = i == index_lower ? lower % koo::block_num_entries : 0;
  size_t pos_block_upper = i == index_upper ? upper % koo::block_num_entries : koo::block_num_entries - 1;

  // Read corresponding entries
  size_t read_size = (pos_block_upper - pos_block_lower + 1) * koo::entry_size;
  static char scratch[4096];
  Slice entries;
  RandomAccessFile* file = tf->file;
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
  saver(arg, key, value);

	Saver* saver_ = reinterpret_cast<Saver*>(arg);
	if (isMergedModel && saver_->state == kNotFound) {		// Linear search on data blocks + retrain if needed
#if AC_TEST2
		std::chrono::system_clock::time_point StartTime2 = std::chrono::system_clock::now();
#endif
		if (left > pos_block_lower && left < pos_block_upper) {			// wrong table
			cache_->Release(handle);
			return s;
		}

		uint64_t max_upper = model->MaxPosition();
		size_t index_max = (max_upper+1) / koo::block_num_entries;
		if ((max_upper+1) % koo::block_num_entries == 0) index_max--;
		if ((index_upper == 0 && left == pos_block_lower)
				|| (index_lower == index_max && left == pos_block_upper)) {
			cache_->Release(handle);
			return s;
		}

		// Decide direction (go prev or next)
		uint64_t i_ = i;
		bool next = true;
		if (index_lower == index_upper) {
			if (left == pos_block_lower) {
				next = false;
				if (pos_block_lower == 0) i_--;
			} else {
				if (pos_block_upper == koo::block_num_entries-1) i_++;
			}
		} else {
			if (i == index_lower) {
				if (left == pos_block_upper && pos_block_lower != pos_block_upper) {		// wrong table
					cache_->Release(handle);
					return s;
				}
				next = false;
			} else {
				if (left == pos_block_lower && pos_block_lower != pos_block_upper) {		// wrong table
					cache_->Release(handle);
					return s;
				}
			}
		}
#if AC_TEST2
		koo::merged_model_miss++;
#endif

		// Find i_ using index block
		Block* index_block = tf->table->rep_->index_block;
		while (true) {
			if (!next) {
				if (i_ == 0) break;
				else i_--;
			}

			uint32_t mid_index_entry = DecodeFixed32(index_block->data_ + index_block->restart_offset_ + i_ * sizeof(uint32_t));
	    uint32_t shared, non_shared, value_length;
		  const char* key_ptr = DecodeEntry(index_block->data_ + mid_index_entry,
	                                      index_block->data_ + index_block->restart_offset_, &shared, &non_shared, &value_length);
			assert(key_ptr != nullptr && shared == 0 && "Index Entry Corruption");
	    Slice mid_key(key_ptr, non_shared);
		  int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);

		  if (next) {
		  	if (comp >= 0) break;
		  	else i_++;
		  	if (i_ >= index_max) break;
			} else {
				if (comp < 0) {
					i_++;
					break;
				}
			}
		}

	  // Check Filter Block
		block_offset = i_ * koo::block_size;
	  if (i != i_ && filter != nullptr && !filter->KeyMayMatch(block_offset, k)) {
		  cache_->Release(handle);
			return s;
	  }

	  // Search interval or the whole data block i_
	  size_t pos_block_lower2, pos_block_upper2;
	  if (i != i_) {
	  	pos_block_lower2 = 0;
	  	pos_block_upper2 = (i_ == index_max) ? max_upper % koo::block_num_entries : koo::block_num_entries - 1;
		} else {
			if (next) {
				pos_block_lower2 = pos_block_upper + 1;
				pos_block_upper2 = (i_ == index_max) ? max_upper % koo::block_num_entries : koo::block_num_entries - 1;
				if (pos_block_lower2 > pos_block_upper2) {
					cache_->Release(handle);
					return s;
				}
			} else {
				pos_block_lower2 = 0;
				pos_block_upper2 = pos_block_lower - 1;
			}
		}

#if RETRAIN && !RETRAIN2
		// Trigger retraining
		if (model->SetRetraining()) {
			FileMetaData* meta_ = new FileMetaData();
			meta_->number = file_number;
			meta_->file_size = meta->file_size;
			meta_->smallest = meta->smallest;
			meta_->largest = meta->largest;
#if RETRAIN3
			env_->PrepareLearning(level, meta_, false);
#else
			env_->PrepareLearning(level, meta_);
#endif
		}
#if AC_TEST
		koo::num_tryretraining++;
#endif
#endif
#if RETRAIN2 || LOOKUP_ACCURACY
		uint64_t last_left = left;
#endif

		// Read corresponding entries
	  read_size = (pos_block_upper2 - pos_block_lower2 + 1) * koo::entry_size;
		static char scratch2[4096];
	  Slice entries2;
		s = file->Read(block_offset + pos_block_lower2 * koo::entry_size, read_size, &entries2, scratch2);
	  assert(s.ok());

	  // Binary Search within the interval
		uint64_t left = pos_block_lower2, right = pos_block_upper2;
	  while (left < right) {
		  uint32_t mid = (left + right) / 2;
			uint32_t shared, non_shared, value_length;
	    const char* key_ptr = DecodeEntry(entries2.data() + (mid - pos_block_lower2) * koo::entry_size,
		          entries2.data() + read_size, &shared, &non_shared, &value_length);
			assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
	    Slice mid_key(key_ptr, non_shared);
		  int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
			if (comp < 0) left = mid + 1;
	    else right = mid;
		}

#if RETRAIN2
		double extra_error = -1;
		if (i == i_) {
			if (next) extra_error = left - pos_block_lower2 + 1;
			else extra_error = pos_block_upper2 - left + 1;
		} else if (std::abs(int(i - i_)) == 1) {
			if (i_ < i) extra_error = last_left + koo::block_num_entries - left + 1;
			else extra_error = left + koo::block_num_entries - last_left + 1;
		}
		double cur_error = model->GetError();
		if (extra_error == -1 ||
				extra_error + cur_error > 51) {		// Retrain
			model->SetError(cur_error, 51 - cur_error);		// run_sosd seterror
			if (model->SetRetraining()) {
				FileMetaData* meta_ = new FileMetaData();
				meta_->number = file_number;
				meta_->file_size = meta->file_size;
				meta_->smallest = meta->smallest;
				meta_->largest = meta->largest;
#if RETRAIN3
				env_->PrepareLearning(level, meta_, false);
#else
				env_->PrepareLearning(level, meta_);
#endif
			}
#if AC_TEST
			koo::num_tryretraining++;
#endif
		}
		else {
			if (extra_error+10+cur_error > 51) model->SetError(cur_error, extra_error);
			else model->SetError(cur_error, extra_error+10);
#if AC_TEST
			koo::num_erroradded++;
#endif
		}
#endif

		// decode the target entry to get the key and value (actually value_addr)
	  uint32_t shared, non_shared, value_length;
		const char* key_ptr = DecodeEntry(entries2.data() + (left - pos_block_lower2) * koo::entry_size,
			      entries2.data() + read_size, &shared, &non_shared, &value_length);
	  assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
		Slice key(key_ptr, non_shared), value(key_ptr + non_shared, value_length);
	  saver(arg, key, value);

#if AC_TEST2
		std::chrono::nanoseconds nano2 = std::chrono::system_clock::now() - StartTime2;
		koo::linear_time += nano2.count();
		koo::linear_num++;

		Saver* saver_ = reinterpret_cast<Saver*>(arg);
		if (saver_->state == kFound) koo::served_m_linear++;
		else koo::served_m_linear_fail++;
#endif

		cache_->Release(handle);
#if LOOKUP_ACCURACY
#if !RETRAIN2
		double extra_error = -1;
		if (i == i_) {
			if (next) extra_error = left - pos_block_lower2 + 1;
			else extra_error = pos_block_upper2 - left + 1;
		} else if (std::abs(int(i - i_)) == 1) {
			if (i_ < i) extra_error = last_left + koo::block_num_entries - left + 1;
			else extra_error = left + koo::block_num_entries - last_left + 1;
		}
		double cur_error = model->GetError();
#endif
		if (extra_error == -1) {
			if (i == i_) {
				if (next) extra_error = left - pos_block_lower2 + 1;
				else extra_error = pos_block_upper2 - left + 1;
			} else {
				int index_diff = std::abs(int(i) - int(i_));
				if (i_ < i) extra_error = last_left + koo::block_num_entries * index_diff - left + 1;
				else extra_error = left + koo::block_num_entries * index_diff - last_left + 1;
				//fprintf(stderr, "ERRROORR extra_error == -1, i: %lu, i_: %lu\n", i, i_);
				//fprintf(stderr, "\textra_error: %f\n", extra_error);
			}
		}
		size_t diff_abs = cur_error + extra_error;
		koo::mm_num_error[level]++;
		koo::mm_error[level] += diff_abs;
		// mutex로 막아야 하나??
		std::ofstream ofs("/koo/HyperLearningless3/koo/data/MM_lookup_error_"+std::to_string(level)+".txt", std::ios::app);
		ofs << diff_abs << std::endl;
		ofs.close();
#endif
		return s;
	}

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
	if (isMergedModel) {
		koo::mm_num_error[level]++;
		koo::mm_error[level] += diff_abs;
		std::ofstream ofs("/koo/HyperLearningless3/koo/data/MM_lookup_error_"+std::to_string(level)+".txt", std::ios::app);
		ofs << diff_abs << std::endl;
		ofs.close();
	} else {
		if (model->is_retrained_)	{
			koo::rm_num_error[level]++;
			koo::rm_error[level] += diff_abs;
			std::ofstream ofs("/koo/HyperLearningless3/koo/data/RM_lookup_error_"+std::to_string(level)+".txt", std::ios::app);
			ofs << diff_abs << std::endl;
			ofs.close();
		} else {
			koo::lm_num_error[level]++;
			koo::lm_error[level] += diff_abs;
			std::ofstream ofs("/koo/HyperLearningless3/koo/data/LM_lookup_error_"+std::to_string(level)+".txt", std::ios::app);
			ofs << diff_abs << std::endl;
			ofs.close();
		}
	}
#endif
	return s;
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

}  // namespace leveldb
