#ifndef LEVELDB_UTIL_H
#define LEVELDB_UTIL_H

#include <vector>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <map>
#include "db/db_impl.h"
#include "hyperleveldb/slice.h"
#include "hyperleveldb/env.h"
#include <x86intrin.h>
#include "koo/Counter.h"
#include "koo/CBModel_Learn.h"
#include "koo/koo.h"
#include "koo/stats.h"

using std::string;
using std::vector;
using std::map;
using leveldb::Slice;

namespace koo {

	class FileLearnedIndexData;
	class LearnedIndexData;
	class FileStats;
	class FileStatsData;

	//extern int MOD;
	extern uint32_t level_model_error;
	extern FileLearnedIndexData* file_data;
	extern CBModel_Learn* learn_cb_model;
	extern leveldb::Env* env;
	extern leveldb::DBImpl* db;
	extern leveldb::ReadOptions read_options;
	extern leveldb::WriteOptions write_options;
	extern uint64_t initial_time;

	//extern uint64_t fd_limit;
	extern bool fresh_write;
	extern bool block_num_entries_recorded;
	extern uint64_t block_num_entries;
	extern uint64_t block_size;
	extern uint64_t entry_size;
	extern float reference_frequency;
	extern uint64_t learn_trigger_time;
	extern int policy;
	extern int level_allowed_seek;
	extern int file_allowed_seek;

	extern FileStatsData* file_stats_data;

	uint64_t SliceToInteger(const Slice& slice);

  // data structure containing infomation for CBA
  class FileStats {
  public:
    uint64_t start;
    uint64_t end;
    std::atomic<int> level;
    std::atomic<uint32_t> num_lookup_neg;
    std::atomic<uint32_t> num_lookup_pos;
    std::atomic<uint64_t> size;

    explicit FileStats(int level_, uint64_t size_) : start(0), end(0) {
    	level.store(level_);
    	num_lookup_pos.store(0);
    	num_lookup_neg.store(0);
    	size.store(size_);
      koo::Stats* instance = koo::Stats::GetInstance();
      uint32_t dummy;
      start = (__rdtscp(&dummy) - instance->initial_time) / koo::reference_frequency;
    };

    void Finish() {
      koo::Stats* instance = koo::Stats::GetInstance();
      uint32_t dummy;
      end = (__rdtscp(&dummy) - instance->initial_time) / koo::reference_frequency;
    }

  };

	class FileStatsData {
		public:
			//leveldb::port::Mutex mutex;
			koo::RWLock rw_lock_;
			std::vector<FileStats*> file_stats;

			void InsertFileStats(int number, int level, uint64_t size) {
				if (file_stats.size() <= number) {
					//mutex.Lock();
					rw_lock_.LockWrite();
					if (file_stats.size() <= number) {
						file_stats.resize(number + 100, nullptr);
						file_stats[number] = new FileStats(level, size);
						//mutex.Unlock();
						rw_lock_.UnlockWrite();
						return;
					}
					//mutex.Unlock();
					rw_lock_.UnlockWrite();
				}
				if (file_stats[number] == nullptr) {
					//mutex.Lock();
					rw_lock_.LockWrite();
					if (file_stats[number] == nullptr) {
						file_stats[number] = new FileStats(level, size);
						//mutex.Unlock();
						rw_lock_.UnlockWrite();
						return;
					}
					//mutex.Unlock();
					rw_lock_.UnlockWrite();
				}
			}

			FileStats* GetFileStats(int number) {
				rw_lock_.LockRead();
				if (file_stats.size() <= number) {
					rw_lock_.UnlockRead();
					return nullptr;
				}
				if (file_stats[number] == nullptr) {
					rw_lock_.UnlockRead();
					return nullptr;
				}
				rw_lock_.UnlockRead();
				return file_stats[number];
			}

			void DeleteFileStats(int number) {
				rw_lock_.LockWrite();
				delete file_stats[number];
				file_stats[number] = nullptr;
				rw_lock_.UnlockWrite();
			}

			~FileStatsData() {
				for (auto fs : file_stats) {
					if (fs != nullptr)
						delete fs;
				}
			}
	};


}

#endif	// LEVELDB_UTIL_H
