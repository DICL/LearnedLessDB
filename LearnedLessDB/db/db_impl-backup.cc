// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_LIMIT_MACROS

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/replay_iterator.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "hyperleveldb/db.h"
#include "hyperleveldb/env.h"
#include "hyperleveldb/replay_iterator.h"
#include "hyperleveldb/status.h"
#include "hyperleveldb/table.h"
#include "hyperleveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "koo/koo.h"
#include "koo/merge.h"

namespace leveldb {

const unsigned kStraightReads = 10;

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  port::CondVar cv_;
  bool linked_;
  bool has_imm_;
  bool wake_me_when_head_;
  bool block_if_backup_in_progress_;
  Writer* prev_;
  Writer* next_;
  uint64_t micros_;
  uint64_t start_sequence_;
  uint64_t end_sequence_;
  MemTable* mem_;
  SHARED_PTR<WritableFile> logfile_;
  SHARED_PTR<log::Writer> log_;

  explicit Writer(port::Mutex* mtx)
    : cv_(mtx),
      linked_(false),
      has_imm_(false),
      wake_me_when_head_(false),
      block_if_backup_in_progress_(true),
      prev_(NULL),
      next_(NULL),
      micros_(0),
      start_sequence_(0),
      end_sequence_(0),
      mem_(NULL),
      logfile_(),
      log_() {
  }
  ~Writer() throw () {
    // must do in order: log, logfile
    if (log_) {
      assert(logfile_);
      log_.reset();
      logfile_.reset();
    }

    // safe because Unref is synchronized internally
    if (mem_) {
      mem_->Unref();
    }
  }
 private:
  Writer(const Writer&);
  Writer& operator = (const Writer&);
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    Output() : number(), file_size(), smallest(), largest() {}
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
#if MERGE
		koo::MergeModel* merge;
#endif
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

#if MERGE
	bool merge_model;
	std::vector<uint64_t> x_lasts;
	int x_lasts_size;
	int cur_x_lasts;
	uint32_t num_keys;
#endif
#if MERGE_BACKGROUND
	int cur_input[2];
#endif

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(),
        outputs(),
        outfile(NULL),
        builder(NULL),
#if MERGE
				merge_model(false),
				x_lasts_size(0),
				cur_x_lasts(0),
				num_keys(0),
#endif
        total_bytes(0) {
#if MERGE_BACKGROUND
		cur_input[0] = 0;
		cur_input[1] = 0;
#endif
  }
 private:
  CompactionState(const CompactionState&);
  CompactionState& operator = (const CompactionState&);
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(),
      db_lock_(NULL),
      mutex_(),
      shutting_down_(NULL),
      mem_(new MemTable(internal_comparator_)),
      imm_(NULL),
      has_imm_(),
      logfile_(),
      logfile_number_(0),
      log_(),
      seed_(0),
      writers_mutex_(),
      writers_upper_(0),
      writers_tail_(NULL),
      snapshots_(),
      pending_outputs_(),
      allow_background_activity_(false),
      num_bg_threads_(0),
      bg_fg_cv_(&mutex_),
      bg_compaction_cv_(&mutex_),
      bg_memtable_cv_(&mutex_),
      bg_log_cv_(&mutex_),
      bg_log_occupied_(false),
      manual_compaction_(NULL),
      manual_garbage_cutoff_(raw_options.manual_garbage_collection ?
                             SequenceNumber(0) : kMaxSequenceNumber),
      replay_iters_(),
      straight_reads_(0),
      versions_(),
      backup_cv_(&writers_mutex_),
      backup_in_progress_(),
      backup_waiters_(0),
      backup_waiter_has_it_(false),
      backup_deferred_delete_(),
      bg_error_() {
  mutex_.Lock();
  mem_->Ref();
  has_imm_.Release_Store(NULL);
  backup_in_progress_.Release_Store(NULL);
  env_->StartThread(&DBImpl::CompactMemTableWrapper, this);
  env_->StartThread(&DBImpl::CompactLevelWrapper, this);
#if MULTI_COMPACTION
  env_->StartThread(&DBImpl::CompactLevelWrapper, this);
  env_->StartThread(&DBImpl::CompactLevelWrapper, this);
  env_->StartThread(&DBImpl::CompactLevelWrapper, this);
  num_bg_threads_ = 5;
#else
  num_bg_threads_ = 2;
#endif
#if VLOG
	koo::db = this;
	vlog = new koo::VLog(dbname_ + "/vlog.txt");
#endif

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);

#if !MULTI_COMPACTION
  for (unsigned i = 0; i < leveldb::config::kNumLevels; ++i) {
    levels_locked_[i] = false;
  }
#endif
  mutex_.Unlock();
  writers_mutex_.Lock();
  writers_mutex_.Unlock();
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  bg_compaction_cv_.SignalAll();
  bg_memtable_cv_.SignalAll();
  while (num_bg_threads_ > 0) {
    bg_fg_cv_.Wait();
  }

#if VLOG
	//CompactMemTableThread();
	//koo::db->vlog->Sync();			// TODO read_cold.cc에선 쓰는데 필요한가?
#endif

  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }


#if AC_TEST
	if (koo::num_files_flush) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# generated files by flush = " << koo::num_files_flush << std::endl;
		std::cout << "# generated files by compaction = " << koo::num_files_compaction << std::endl;
		std::cout << "# learned files = " << koo::num_learned << std::endl;
		std::cout << "# merged files = " << koo::num_merged << std::endl;
		std::cout << "# retrained files = " << koo::num_retrained << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if AC_TEST2
	if (koo::served_i || koo::served_l) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# Index block served = " << koo::served_i << std::endl;
		std::cout << "# Learned model served = " << koo::served_l << std::endl;
		std::cout << "# Merged model served = " << koo::served_m << std::endl;
		std::cout << "# Merged model linear search served = " << koo::served_m_linear << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_W
	if (koo::num_learntime) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Avg compaction time = " << std::to_string((double)koo::compactiontime/(double)koo::num_compactiontime) << " ns,\t# compactions = " << koo::num_compactiontime << ",\tTotal compaction time = " << koo::compactiontime << std::endl;
#if AC_TEST
		std::cout << "Avg compaction time per file = " << std::to_string((double)koo::compactiontime/(double)koo::num_files_compaction) << " ns\n";
#endif
		std::cout << "Avg learning time per file = " << std::to_string((double)koo::learntime/(double)koo::num_learntime) << " ns,\t# learned files = " << koo::num_learntime << ",\tTotal learning time = " << std::to_string(koo::learntime) << std::endl;
#if MERGE_BACKGROUND
		std::cout << "Avg merging time per file = " << std::to_string((double)koo::mergetime/(double)koo::num_mergetime) << " ns,\t# merged files = " << koo::num_mergetime << std::endl;
#else
		std::cout << "Avg merging time per compaction = " << std::to_string((double)koo::mergetime/(double)koo::num_mergetime) << " ns,\t# compactions with merging = " << koo::num_mergetime << ",\tTotal merging time = " << std::to_string(koo::mergetime) << std::endl;
		std::cout << "\tAvg merging time per file = " << std::to_string((double)koo::mergetime/(double)koo::num_merge_size) << " ns,\t# merged files = " << koo::num_merge_size << std::endl;
#endif
		std::cout << "\nAvg # of items to learn = " << std::to_string((double)koo::learn_size/(double)koo::num_learn_size) << ",\tnum = " << koo::num_learn_size << std::endl;
		std::cout << "Avg # of items to merge = " << std::to_string((double)koo::merge_size/(double)koo::num_merge_size) << ",\tnum = " << koo::num_merge_size << std::endl;
		std::cout << "\nAvg learning time per L0 file = " << std::to_string((double)koo::learntime_l0/(double)koo::num_learntime_l0) << " ns,\t# learned L0 files = " << koo::num_learntime_l0 << ",\tTotal L0 files learning time = " << std::to_string(koo::learntime_l0) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_R
	if (koo::num_i_path) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Index block path avg time = " << std::to_string(koo::i_path/((double)koo::num_i_path)) << " ns,\t# index block path = " << koo::num_i_path << ",\tTotal time = " << std::to_string(koo::i_path) << std::endl;
		std::cout << "Learned model path avg time = " << std::to_string(koo::l_path/((double)koo::num_l_path)) << " ns,\t# learned model path = " << koo::num_l_path << ",\tTotal time = " << std::to_string(koo::l_path) << std::endl;
		std::cout << "Merged model path avg time = " << std::to_string(koo::m_path/((double)koo::num_m_path)) << " ns,\t# merged model path = " << koo::num_m_path << ",\tTotal time = " << std::to_string(koo::m_path) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
#if TIME_R_LEVEL
		for (unsigned l=0; l<config::kNumLevels; l++) {
			if (koo::num_i_path_l[l] == 0 && koo::num_l_path_l[l] == 0 && koo::num_m_path_l[l] == 0) continue;
			std::cout << "[ Level " << l << " ]\n";
			std::cout << "Index block path avg time = " << std::to_string(koo::i_path_l[l]/((double)koo::num_i_path_l[l])) << " ns,\t# index block path = " << koo::num_i_path_l[l] << ",\tTotal time = " << std::to_string(koo::i_path_l[l]) << std::endl;
			std::cout << "Learned model path avg time = " << std::to_string(koo::l_path_l[l]/((double)koo::num_l_path_l[l])) << " ns,\t# learned model path = " << koo::num_l_path_l[l] << ",\tTotal time = " << std::to_string(koo::l_path_l[l]) << std::endl;
			std::cout << "Merged model path avg time = " << std::to_string(koo::m_path_l[l]/((double)koo::num_m_path_l[l])) << " ns,\t# merged model path = " << koo::num_m_path_l[l] << ",\tTotal time = " << std::to_string(koo::m_path_l[l]) << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
#endif
	}
#endif
#if TIME_R_DETAIL
	if (koo::num_mem) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "[ MEM ]\nTotal num: " << koo::num_mem << ",\tFound num: " << koo::num_mem_succ << ",\tTotal time: " << std::to_string(koo::time_mem) << " ns,\tAVG: " << std::to_string(koo::time_mem/((double)koo::num_mem)) << " ns" << std::endl;
		std::cout << "[ IMM ]\nTotal num: " << koo::num_imm << ",\tFound num: " << koo::num_imm_succ << ",\tTotal time: " << std::to_string(koo::time_imm) << " ns,\tAVG: " << std::to_string(koo::time_imm/((double)koo::num_imm)) << " ns" << std::endl;
		std::cout << "[ VER ]\nTotal num: " << koo::num_ver << ",\tFound num: " << koo::num_ver_succ << ",\tTotal time: " << std::to_string(koo::time_ver) << " ns,\tAVG: " << std::to_string(koo::time_ver/((double)koo::num_ver)) << " ns" << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if MULTI_COMPACTION_CNT
	if (koo::num_PickCompaction) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# PickCompactionLevel() called: " << koo::num_PickCompactionLevel << std::endl;
		std::cout << "# PickCompaction() called: " << koo::num_PickCompaction << std::endl;
		std::cout << "# Compactions: " << koo::num_compactions << std::endl;
		std::cout << "# Compaction output files: " << koo::num_output_files << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if EH_AC_TEST
	if (koo::num_eh_insert) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# succeed insert: " << koo::num_eh_insert << "\t# failed insert: " << koo::num_eh_insert_fail << std::endl;
		std::cout << "# succeed get: " << koo::num_eh_get << "\t# failed get: " << koo::num_eh_get_fail << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if SST_LIFESPAN
	if (!koo::lifespans.empty()) {
		/*int64_t total_T_time[config::kNumLevels] = {0, };
		uint32_t num_T_time[config::kNumLevels] = {0, };
		int64_t total_M_time[config::kNumLevels] = {0, };
		uint32_t num_M_time[config::kNumLevels] = {0, };
		int64_t total_M_time_pos[config::kNumLevels] = {0, };
		uint32_t num_M_time_pos[config::kNumLevels] = {0, };
		int64_t  total_U_time[config::kNumLevels] = {0, };
		uint32_t num_U_time[config::kNumLevels] = {0, };
		std::ofstream ofs("/koo/HyperLearningless/koo/data/lifespans.txt");*/
		std::vector<std::pair<uint64_t, koo::FileLifespanData>> lss[config::kNumLevels];
		for (auto& ls : koo::lifespans) lss[ls.second.level].push_back(std::make_pair(ls.first, ls.second));
		/*for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			ofs << "[ Level " << i << " Tables ] num: " << lss[i].size() << std::endl;
			for (auto& ls : lss[i]) {
				ofs << ls.first << "\t(level " << i << "):\t" << ls.second.T_start << " " << ls.second.T_end << " " << ls.second.M_end << " " << ls.second.U_end << std::endl;
				ofs << "\t\tT_time: " << ls.second.T_end - ls.second.T_start;
				total_T_time[i] += ls.second.T_end - ls.second.T_start;
				num_T_time[i]++;
				if (ls.second.M_end != 0) {			// not L0 files
					ofs << "\t\tM_time: " << (int)(ls.second.M_end - ls.second.T_end);
					total_M_time[i] += (int)(ls.second.M_end - ls.second.T_end);
					num_M_time[i]++;
					if ((int)(ls.second.M_end - ls.second.T_end) > 0) {
						total_M_time_pos[i] += (int)(ls.second.M_end - ls.second.T_end);
						num_M_time_pos[i]++;
					}
					if (ls.second.U_end != 0) {			// files built when workload is ending
						ofs << "\t\tU_time: " << ls.second.U_end - ls.second.M_end;
						total_U_time[i] += ls.second.U_end - ls.second.M_end;
						num_U_time[i]++;
					}
				}
				ofs << std::endl;
			}
			ofs << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			std::cout << "[ Level " << i << " Tables ] num: " << lss[i].size() << std::endl;
			std::cout << "\tAvg. T_time: " << std::to_string(total_T_time[i]/(double)num_T_time[i]) << ",\t\tTotal: " << total_T_time[i] << ",\tnum: " << num_T_time[i] << std::endl;
			std::cout << "\tAvg. M_time: " << std::to_string(total_M_time[i]/(double)num_M_time[i]) << ",\t\tTotal: " << total_M_time[i] << ",\tnum: " << num_M_time[i] << std::endl;
			std::cout << "\tAvg. positive M_time: " << std::to_string(total_M_time_pos[i]/(double)num_M_time_pos[i]) << ",\t\tTotal: " << total_M_time_pos[i] << ",\tnum: " << num_M_time_pos[i] << std::endl;
			std::cout << "\tAvg. U_time: " << std::to_string(total_U_time[i]/(double)num_U_time[i]) << ",\t\tTotal: " << total_U_time[i] << ",\tnum: " << num_U_time[i] << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;*/
		int64_t total_T_time = 0;
		int64_t total_M_time = 0;
		int64_t total_U_time = 0;
		uint32_t num_time = 0, num_U_end0 = 0;
		for (int l=0; l<config::kNumLevels; l++) {
			if (lss[l].empty()) continue;
			for (auto& ls : lss[l]) {
				if (!ls.second.M_end) continue;
				if (!ls.second.U_end) { num_U_end0++; continue; }
				total_T_time += ls.second.T_end - ls.second.T_start;
				total_M_time += (int64_t)(ls.second.M_end - ls.second.T_end);
				total_U_time += (int64_t)(ls.second.U_end - ls.second.M_end);
				num_time++;
			}
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# learned tables: " << num_time << ", # tables learned when workload is ending: " << num_U_end0 << std::endl;
		std::cout << "Avg. T_time: " << std::to_string(total_T_time/(double)num_time) << ",\tTotal T_time: " << total_T_time << std::endl;
		std::cout << "Avg. M_time: " << std::to_string(total_M_time/(double)num_time) << ",\tTotal M_time: " << total_M_time << std::endl;
		std::cout << "Avg. U_time: " << std::to_string(total_U_time/(double)num_time) << ",\tTotal U_time: " << total_U_time << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#if SST_LIFESPAN_DETAIL
	if (!koo::lifespans.empty()) {
		int64_t total_T_time[2][config::kNumLevels] = {0, };
		uint32_t num_T_time[2][config::kNumLevels] = {0, };
		int64_t total_M_time[2][config::kNumLevels] = {0, };
		uint32_t num_M_time[2][config::kNumLevels] = {0, };
		int64_t total_M_time_pos[2][config::kNumLevels] = {0, };
		uint32_t num_M_time_pos[2][config::kNumLevels] = {0, };
		int64_t  total_U_time[2][config::kNumLevels] = {0, };
		uint32_t num_U_time[2][config::kNumLevels] = {0, };
		std::vector<std::pair<uint64_t, koo::FileLifespanData>> lss[config::kNumLevels];
		for (auto& ls : koo::lifespans) lss[ls.second.level].push_back(std::make_pair(ls.first, ls.second));
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			for (auto& ls : lss[i]) {
				int m = ls.second.merged ? 1 : 0;
				total_T_time[m][i] += ls.second.T_end - ls.second.T_start;
				num_T_time[m][i]++;
				if (ls.second.M_end != 0) {
					total_M_time[m][i] += (int)(ls.second.M_end - ls.second.T_end);
					num_M_time[m][i]++;
					if ((int)(ls.second.M_end - ls.second.T_end) > 0) {
						total_M_time_pos[m][i] += (int)(ls.second.M_end - ls.second.T_end);
						num_M_time_pos[m][i]++;
					}
					if (ls.second.U_end != 0) {			// files built when workload is ending
						total_U_time[m][i] += ls.second.U_end - ls.second.M_end;
						num_U_time[m][i]++;
					}
				}
			}
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			std::cout << "[ Level " << i << " Tables ] num: " << lss[i].size() << std::endl;
			std::cout << "\t< Learning >\n";
			std::cout << "\t\tAvg. T_time: " << std::to_string(total_T_time[0][i]/(double)num_T_time[0][i]) << ",\t\tTotal: " << total_T_time[0][i] << ",\tnum: " << num_T_time[0][i] << std::endl;
			std::cout << "\t\tAvg. M_time: " << std::to_string(total_M_time[0][i]/(double)num_M_time[0][i]) << ",\t\tTotal: " << total_M_time[0][i] << ",\tnum: " << num_M_time[0][i] << std::endl;
			std::cout << "\t\tAvg. positive M_time: " << std::to_string(total_M_time_pos[0][i]/(double)num_M_time_pos[0][i]) << ",\t\tTotal: " << total_M_time_pos[0][i] << ",\tnum: " << num_M_time_pos[0][i] << std::endl;
			std::cout << "\t\tAvg. U_time: " << std::to_string(total_U_time[0][i]/(double)num_U_time[0][i]) << ",\t\tTotal: " << total_U_time[0][i] << ",\tnum: " << num_U_time[0][i] << std::endl;
			std::cout << "\t< Merging >\n";
			std::cout << "\t\tAvg. T_time: " << std::to_string(total_T_time[1][i]/(double)num_T_time[1][i]) << ",\t\tTotal: " << total_T_time[1][i] << ",\tnum: " << num_T_time[1][i] << std::endl;
			std::cout << "\t\tAvg. M_time: " << std::to_string(total_M_time[1][i]/(double)num_M_time[1][i]) << ",\t\tTotal: " << total_M_time[1][i] << ",\tnum: " << num_M_time[1][i] << std::endl;
			std::cout << "\t\tAvg. positive M_time: " << std::to_string(total_M_time_pos[1][i]/(double)num_M_time_pos[1][i]) << ",\t\tTotal: " << total_M_time_pos[1][i] << ",\tnum: " << num_M_time_pos[1][i] << std::endl;
			std::cout << "\t\tAvg. U_time: " << std::to_string(total_U_time[1][i]/(double)num_U_time[1][i]) << ",\t\tTotal: " << total_U_time[1][i] << ",\tnum: " << num_U_time[1][i] << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#endif

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  log_.reset();
  logfile_.reset();
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
#if LEARN
	delete koo::file_data;
#endif
#if VLOG
	delete vlog;
#endif
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  ConcurrentWritableFile* file;
  Status s = env_->NewConcurrentWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  // Defer if there's background activity
  mutex_.AssertHeld();
  if (backup_in_progress_.Acquire_Load() != NULL) {
    backup_deferred_delete_ = true;
    return;
  }

  // If you ever release mutex_ in this function, you'll need to do more work in
  // LiveBackup

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
        default:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
#if SST_LIFESPAN
				//assert(koo::lifespans.find(number) != koo::lifespans.end());
				if (koo::lifespans.find(number) != koo::lifespans.end()) {
					koo::lifespans[number].U_end = env_->NowMicros();
				}
#endif
#if LEARN
				/*if (type == kTableFile) {
					koo::file_data->DeleteModel(number);
				}*/
#endif
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover();
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
          logs.push_back(number);
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    LogReporter()
      : env(),
        info_log(),
        fname(),
        status() {
    }
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
   private:
    LogReporter(const LogReporter&);
    LogReporter& operator = (const LogReporter&);
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit, NULL, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit, NULL, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base, uint64_t* number) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  if (number) {
    *number = meta.number;
  }
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);
#if SST_LIFESPAN
	assert(koo::lifespans.find(*number) == koo::lifespans.end());
	koo::lifespans.insert({*number, koo::FileLifespanData(0, start_micros)});
#endif

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
#if MULTI_COMPACTION
			level = 0;
#else
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
      while (level > 0 && levels_locked_[level]) {
        --level;
      }
#endif
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTableThread() {
  MutexLock l(&mutex_);
  while (!shutting_down_.Acquire_Load() && !allow_background_activity_) {
    bg_memtable_cv_.Wait();
  }
#if LEARNING_IO
	std::cout << "Flush thread id: " << syscall(SYS_gettid) << std::endl;
#endif
  while (!shutting_down_.Acquire_Load()) {
    while (!shutting_down_.Acquire_Load() && imm_ == NULL) {
      bg_memtable_cv_.Wait();
    }
    if (shutting_down_.Acquire_Load()) {
      break;
    }

    // Save the contents of the memtable as a new Table
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    uint64_t number;
    Status s = WriteLevel0Table(imm_, &edit, base, &number);
    base->Unref(); base = NULL;

    if (s.ok() && shutting_down_.Acquire_Load()) {
      s = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
      edit.SetPrevLogNumber(0);
      edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
      s = versions_->LogAndApply(&edit, &mutex_, &bg_log_cv_, &bg_log_occupied_);
#if SST_LIFESPAN
			assert(koo::lifespans.find(number) != koo::lifespans.end());
			koo::lifespans[number].T_end = env_->NowMicros();
#endif
    }

    pending_outputs_.erase(number);

    if (s.ok()) {
      // Commit to the new state
      imm_->Unref();
      imm_ = NULL;
      has_imm_.Release_Store(NULL);
      bg_fg_cv_.SignalAll();
      bg_compaction_cv_.Signal();
      DeleteObsoleteFiles();
    } else {
      RecordBackgroundError(s);
      continue;
    }
#if AC_TEST
		koo::num_files_flush++;
#endif

#if LEARN_L0
		int level = edit.new_files_[0].first;
		env_->PrepareLearning(level, new FileMetaData(edit.new_files_[0].second));
#endif

    if (!shutting_down_.Acquire_Load() && !s.ok()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      bg_fg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      Log(options_.info_log, "Waiting after memtable compaction error: %s",
          s.ToString().c_str());
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    assert(config::kL0_SlowdownWritesTrigger > 0);
  }
  Log(options_.info_log, "cleaning up CompactMemTableThread");
  num_bg_threads_ -= 1;
  bg_fg_cv_.SignalAll();
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (unsigned level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(unsigned level, const Slice* begin,const Slice* end) {
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      bg_compaction_cv_.Signal();
      bg_memtable_cv_.Signal();
    } else {  // Running either my compaction or another compaction.
      bg_fg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_fg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::CompactLevelThread() {
  MutexLock l(&mutex_);
  while (!shutting_down_.Acquire_Load() && !allow_background_activity_) {
    bg_compaction_cv_.Wait();
  }
#if LEARNING_IO
	std::cout << "Compaction thread id: " << syscall(SYS_gettid) << std::endl;
#endif
  while (!shutting_down_.Acquire_Load()) {
#if MULTI_COMPACTION
		unsigned level = config::kNumLevels;
    while (!shutting_down_.Acquire_Load() && manual_compaction_ == NULL) {
    	level = versions_->PickCompactionLevel(straight_reads_ > kStraightReads);
#if MULTI_COMPACTION_CNT
			koo::num_PickCompactionLevel++;
#endif
    	if (level != config::kNumLevels) break;
      bg_compaction_cv_.Wait();
		}
#else
    while (!shutting_down_.Acquire_Load() &&
           manual_compaction_ == NULL &&
           !versions_->NeedsCompaction(levels_locked_, straight_reads_ > kStraightReads)) {
      bg_compaction_cv_.Wait();
    }
#endif
    if (shutting_down_.Acquire_Load()) {
      break;
    }

#if MULTI_COMPACTION
    Status s = BackgroundCompaction(level);
#else
    assert(manual_compaction_ == NULL || num_bg_threads_ == 2);
    Status s = BackgroundCompaction();
#endif
    bg_fg_cv_.SignalAll(); // before the backoff In case a waiter
                           // can proceed despite the error

    if (s.ok()) {
      // Success
    } else if (shutting_down_.Acquire_Load()) {
      // Error most likely due to shutdown; do not wait
    } else {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      Log(options_.info_log, "Waiting after background compaction error: %s",
          s.ToString().c_str());
      mutex_.Unlock();
      int seconds_to_sleep = 1;
      env_->SleepForMicroseconds(seconds_to_sleep * 1000000);
      mutex_.Lock();
    }
  }
  Log(options_.info_log, "cleaning up CompactLevelThread");
  num_bg_threads_ -= 1;
  bg_fg_cv_.SignalAll();
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_fg_cv_.SignalAll();
  }
}

#if MULTI_COMPACTION
Status DBImpl::BackgroundCompaction(unsigned level) {
#else
Status DBImpl::BackgroundCompaction() {
#endif
  mutex_.AssertHeld();
  Compaction* c = NULL;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
#if !MULTI_COMPACTION
    unsigned level = versions_->PickCompactionLevel(levels_locked_, straight_reads_ > kStraightReads);
#if MULTI_COMPACTION_CNT
		koo::num_PickCompactionLevel++;
#endif
#endif
    if (level != config::kNumLevels) {
      c = versions_->PickCompaction(versions_->current(), level);
#if MULTI_COMPACTION_CNT
			koo::num_PickCompaction++;
#endif
    }
#if !MULTI_COMPACTION
    if (c) {
      assert(!levels_locked_[c->level() + 0]);
      assert(!levels_locked_[c->level() + 1]);
      levels_locked_[c->level() + 0] = true;
      levels_locked_[c->level() + 1] = true;
		}
#endif
  }

  Status status;

  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove() && c->level() > 0) {
    // Move file to next level
    for (size_t i = 0; i < c->num_input_files(0); ++i) {
      FileMetaData* f = c->input(0, i);
      c->edit()->DeleteFile(c->level(), f->number);
      c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                         f->smallest, f->largest);
    }
    status = versions_->LogAndApply(c->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_);
#if MULTI_COMPACTION_CNT
		koo::num_output_files += c->num_input_files(0);
#endif
#if MULTI_COMPACTION
		c->MarkFilesBeingCompacted(false);
		versions_->UnregisterCompaction(c);
#endif
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    for (size_t i = 0; i < c->num_input_files(0); ++i) {
      FileMetaData* f = c->input(0, i);
      Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number),
          c->level() + 1,
          static_cast<unsigned long long>(f->file_size),
          status.ToString().c_str(),
          versions_->LevelSummary(&tmp));
    }
  } else {
    CompactionState* compact = new CompactionState(c);
#if MERGE
		size_t num_input_files[2] = {c->num_input_files(0), c->num_input_files(1)};
		if (c->level()) {		// Over level 1 compaction
#if TIME_W
			std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
			compact->merge_model = true;

			// Check if input files are valid
			for (int which=0; which<2; which++) {
				for (int i=0; i<num_input_files[which]; i++) {
					uint64_t number = c->input(which, i)->number;
					auto model = koo::file_data->GetModel(number);
					if (!model->Learned() || !model->CheckMergeHistory()) {
						compact->merge_model = false;
						break;
					}
				}
				if (!compact->merge_model) break;
			}

			if (compact->merge_model) {
				// Collect and sort x_lasts
				std::vector<uint64_t>& x_lasts = compact->x_lasts;
				for (int which=0; which<2; which++) {
					for (int i=0; i<num_input_files[which]; i++) {
						uint64_t number = c->input(which, i)->number;
						auto& segs = koo::file_data->GetModel(number)->string_segments;
						for (auto& s : segs) x_lasts.push_back(s.x_last);
						x_lasts.pop_back();			// dummy
					}
				}
				compact->x_lasts_size = x_lasts.size();
				std::sort(x_lasts.begin(), x_lasts.end());		// quick sort
			}
#if TIME_W
			std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
			koo::mergetime += nano.count();
#endif
		}
#endif
#if TIME_W
		std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
    status = DoCompactionWork(compact);
#if TIME_W
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		koo::compactiontime += nano.count();
		koo::num_compactiontime++;
#endif
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
#if MERGE && !MERGE_BACKGROUND
		else {
			if (compact->merge_model) {
#if TIME_W
				std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
				int cur_input[2] = {0, 0};		// 0: level, 1: level+1

				for (auto& output : compact->outputs) {
					koo::MergeModel* merge = output.merge;
					merge->file_number = output.number;
					merge->level = c->level() + 1;
#if YCSB_CXX
					merge->begin_key = (uint64_t) std::stoull(output.smallest.user_key().ToString());
					merge->end_key = (uint64_t) std::stoull(output.largest.user_key().ToString());
#else
					merge->begin_key = atoll(output.smallest.user_key().data());
					merge->end_key = atoll(output.largest.user_key().data());
#endif

					// Find max history
					uint32_t max_history = 1;
					for (int which=0; which<2; which++) {
						while (cur_input[which] < num_input_files[which]) {
							FileMetaData* meta = c->input(which, cur_input[which]);
							if (user_comparator()->Compare(output.smallest.user_key(), meta->largest.user_key()) > 0) {
								cur_input[which]++;
								continue;
							}
							if (user_comparator()->Compare(output.largest.user_key(), meta->smallest.user_key()) < 0)
								break;

							uint32_t next_history = koo::file_data->GetModel(meta->number)->GetMergeHistory() + 1;
							if (next_history > max_history) max_history = next_history;

							if (user_comparator()->Compare(output.largest.user_key(), meta->largest.user_key()) <= 0)
								break;
							cur_input[which]++;
						}
					}

					// Merge
					if (!merge->Merge()) {			// Merging failed
						// Trigger general learning path of Bourbon
						// TODO trigger하지 말까?
						std::cout << __LINE__ << " Merge() Failed\n";

					} else {										// Merging succeed
						// Insert merged new model into koo::file_data
						koo::LearnedIndexData* new_model = koo::file_data->GetModel(output.number);
						new_model->min_key = merge->begin_key;
						new_model->max_key = merge->end_key;
						new_model->level = c->level() + 1;
						new_model->size = output.merge->num_entries;

						new_model->SetMergedModel(merge->segs_output, max_history);
#if SST_LIFESPAN
						assert(koo::lifespans.find(output.number) != koo::lifespans.end());
						koo::lifespans[output.number].M_end = env_->NowMicros();
#if SST_LIFESPAN_DETAIL
						koo::lifespans[output.number].merged = true;
#endif
#endif
#if AC_TEST
						koo::num_merged++;
#endif
#if DEBUG
						Version* current = versions_->current();
						current->Ref();
						current->TestModelAccuracy(output.number, output.file_size);
						current->Unref();
#endif
#if TIME_W
						koo::merge_size += merge->seg_infos.size();
						koo::num_merge_size++;
#endif
					}
					delete merge;
				}
#if TIME_W
				std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
				koo::mergetime += nano.count();
				koo::num_mergetime++;
#endif
			} /*else {			// Cannot merge. Inputs files are invalid
#if AC_TEST
				std::cout << __LINE__ << " Cannot merge. Input files are invalid\n";
#endif
				// Trigger general learning path of Bourbon
				int level = c->level() + 1;
				for (auto& output : compact->outputs) {
					FileMetaData* meta_ = new FileMetaData();
					meta_->number = output.number;
					meta_->file_size = output.file_size;
					meta_->smallest = output.smallest;
					meta_->largest = output.largest;

					env_->PrepareLearning(level, meta_);
					delete output.merge;
				}
			}*/
		}
#endif
    CleanupCompaction(compact);
#if MULTI_COMPACTION
		c->MarkFilesBeingCompacted(false);
		versions_->UnregisterCompaction(c);
#endif
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }

  if (c) {
#if !MULTI_COMPACTION
    levels_locked_[c->level() + 0] = false;
    levels_locked_[c->level() + 1] = false;
#endif
    delete c;
  }

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
  return status;
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
#if MERGE
		out.merge = new koo::MergeModel();
#endif
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }
#if SST_LIFESPAN
	assert(koo::lifespans.find(file_number) == koo::lifespans.end());
	koo::lifespans.insert({file_number, koo::FileLifespanData(
			compact->compaction->level() + 1, env_->NowMicros())});
#endif

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
#if MERGE
	if (compact->merge_model) {
#if TIME_W
		std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
		koo::MergeModel* merge = compact->current_output()->merge;
		merge->num_entries = current_entries;

		auto& seg_infos = merge->seg_infos;
		int cur = compact->cur_x_lasts;
		if (seg_infos.empty()) {
			seg_infos.push_back(std::make_pair(compact->x_lasts[cur], compact->num_keys));
		} else {
			if (compact->num_keys) {
				seg_infos.push_back(std::make_pair(compact->x_lasts[cur], compact->num_keys));
			}
		}
#if TIME_W
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		koo::mergetime += nano.count();
#endif
	}
#endif
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;
#if AC_TEST
	koo::num_files_compaction++;
#endif

#if LEARN
	int level = compact->compaction->level() + 1;
	CompactionState::Output* output = compact->current_output();
#if MERGE
	if (!compact->merge_model) {
#endif
	FileMetaData* meta = new FileMetaData();
	meta->number = output->number;
	meta->file_size = output->file_size;
	meta->smallest = output->smallest;
	meta->largest = output->largest;
	env_->PrepareLearning(level, meta);
#if MERGE
	}
#endif
#if MERGE_BACKGROUND
	else {
		koo::MergeModel* merge = output->merge;		// TODO 그대로 쓰면 안되겠지만 어차피 머지가 delete output보다 먼저 끝날듯
		merge->file_number = output->number;
		merge->level = level;
		merge->begin_key = (uint64_t) std::stoull(output->smallest.user_key().ToString());
		merge->end_key = (uint64_t) std::stoull(output->largest.user_key().ToString());

		// Find max history		// TODO 더 효율적인 방법 없을까
		uint32_t max_history = 1;
		for (int which=0; which<2; which++) {
			size_t num_input_files = compact->compaction->num_input_files(which);
			while (compact->cur_input[which] < num_input_files) {
				FileMetaData* meta = compact->compaction->input(which, compact->cur_input[which]);
				if (user_comparator()->Compare(output->smallest.user_key(), meta->largest.user_key()) > 0) {
					compact->cur_input[which]++;
					continue;
				}
				if (user_comparator()->Compare(output->largest.user_key(), meta->smallest.user_key()) < 0)
					break;

				uint32_t next_history = koo::file_data->GetModel(meta->number)->GetMergeHistory() + 1;
				if (next_history > max_history) max_history = next_history;

				if (user_comparator()->Compare(output->largest.user_key(), meta->largest.user_key()) <= 0)
					break;
				compact->cur_input[which]++;
			}
		}
		merge->history = max_history;

		env_->PrepareMerging(merge);
	}
#endif
#endif

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %lu@%d + %lu@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %lu@%d + %lu@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  ParsedInternalKey current_key;
  std::string current_key_backing;
  bool has_current_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  size_t boundary_hint = 0;
#if MERGE
#if TIME_W
	std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
	std::vector<std::string> x_lasts_str;
	for (auto& x_last : compact->x_lasts) x_lasts_str.push_back(std::to_string(x_last));
#if TIME_W
	std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
	koo::mergetime += nano.count();
#endif
#endif
#if OPT1
	bool pass = true;
#endif
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    Slice key = input->key();
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_key_backing.clear();
      has_current_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_key ||
          user_comparator()->Compare(ikey.user_key,
                                     current_key.user_key) != 0) {
        if (has_current_key && compact->builder &&
            compact->builder->FileSize() >=
            compact->compaction->MinOutputFileSize() &&
            compact->compaction->CrossesBoundary(current_key, ikey, &boundary_hint)) {
          status = FinishCompactionOutputFile(compact, input);
          if (!status.ok()) {
            break;
          }
        }
        // First occurrence of this user key
        current_key_backing.assign(key.data(), key.size());
        bool x = ParseInternalKey(Slice(current_key_backing), &current_key);
        assert(x);
        has_current_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // Just remember that last_sequence_for_key is decreasing over time, and
      // all of this makes sense.

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      // If we're going to drop this key, and there was no previous version of
      // this key, and it was written at or after the garbage cutoff, we keep
      // it.
      if (drop &&
          last_sequence_for_key == kMaxSequenceNumber  &&
          ikey.sequence >= manual_garbage_cutoff_) {
        drop = false;
      }

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
#if MERGE
				if (compact->merge_model) compact->num_keys = 0;
#endif
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());
#if MERGE
			if (compact->merge_model) {
#if TIME_W
				//std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
#if OPT1
				if (pass) pass = false;
				else {
					int& cur = compact->cur_x_lasts;
					if (user_comparator()->Compare(ikey.user_key, Slice(x_lasts_str[cur])) > 0) {
						compact->current_output()->merge->seg_infos.push_back(std::make_pair(compact->x_lasts[cur++], compact->num_keys));
						compact->num_keys = 0;
					}
					compact->num_keys += 2;
					pass = true;
				}
#else
				int& cur = compact->cur_x_lasts;
				if (user_comparator()->Compare(ikey.user_key, Slice(x_lasts_str[cur])) > 0) {
					compact->current_output()->merge->seg_infos.push_back(std::make_pair(compact->x_lasts[cur++], compact->num_keys));
					compact->num_keys = 0;
				}
				compact->num_keys++;
#endif
#if TIME_W
				//std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
				//koo::mergetime += nano.count();
#endif
			}
#endif

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
#if SST_LIFESPAN
		for (auto& output : compact->outputs) {
			assert(koo::lifespans.find(output.number) != koo::lifespans.end());
			koo::lifespans[output.number].T_end = env_->NowMicros();
		}
#endif
#if MULTI_COMPACTION_CNT
		koo::num_output_files += compact->outputs.size();
#endif
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* /*arg2*/) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options, uint64_t number,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed, bool external_sync) {
  IterState* cleanup = new IterState;
  if (!external_sync) {
    mutex_.Lock();
  }
  ++straight_reads_;
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddSomeIterators(options, number, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  if (!external_sync) {
    mutex_.Unlock();
  }
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), 0, &ignored, &ignored_seed, false);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
#if TIME_R_DETAIL
		koo::num_mem++;
		std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
    bool result = mem->Get(lkey, value, &s);
    std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
    koo::time_mem += nano.count();
    if (result) koo::num_mem_succ++;
    else {
    	if (imm != NULL) {
				koo::num_imm++;
				StartTime = std::chrono::system_clock::now();
    		result = imm->Get(lkey, value, &s);
				nano = std::chrono::system_clock::now() - StartTime;
    		koo::time_imm += nano.count();
    		if (result) koo::num_imm_succ++;
    		else {
    			koo::num_ver++;
					StartTime = std::chrono::system_clock::now();
					s = current->Get(options, lkey, value, &stats);
					nano = std::chrono::system_clock::now() - StartTime;
					koo::time_ver += nano.count();
					if (s.ok()) koo::num_ver_succ++;
		      have_stat_update = true;
				}
			} else {
    		koo::num_ver++;
				StartTime = std::chrono::system_clock::now();
				s = current->Get(options, lkey, value, &stats);
				nano = std::chrono::system_clock::now() - StartTime;
				koo::time_ver += nano.count();
				if (s.ok()) koo::num_ver_succ++;
	      have_stat_update = true;
			}
		}
#else
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
#endif
#if VLOG
		if (s.ok()) {
			uint64_t value_address = DecodeFixed64(value->c_str());
			uint32_t value_size = DecodeFixed32(value->c_str() + sizeof(uint64_t));
			*value = std::move(koo::db->vlog->ReadRecord(value_address, value_size));
		}
#endif
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    bg_compaction_cv_.Signal();
  }
  ++straight_reads_;
#if MULTI_COMPACTION
	if (straight_reads_ == 100) bg_compaction_cv_.Signal();
#endif
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, 0, &latest_snapshot, &seed, false);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::GetReplayTimestamp(std::string* timestamp) {
  uint64_t file = 0;
  uint64_t seqno = 0;

  {
    MutexLock l(&mutex_);
    file = versions_->NewFileNumber();
    versions_->ReuseFileNumber(file);
    seqno = versions_->LastSequence();
  }

  WaitOutWriters();
  timestamp->clear();
  PutVarint64(timestamp, file);
  PutVarint64(timestamp, seqno);
}

void DBImpl::AllowGarbageCollectBeforeTimestamp(const std::string& timestamp) {
  Slice ts_slice(timestamp);
  uint64_t file = 0;
  uint64_t seqno = 0;

  if (timestamp == "all") {
    // keep zeroes
  } else if (timestamp == "now") {
    MutexLock l(&mutex_);
    seqno = versions_->LastSequence();
    if (manual_garbage_cutoff_ < seqno) {
      manual_garbage_cutoff_ = seqno;
    }
  } else if (GetVarint64(&ts_slice, &file) &&
             GetVarint64(&ts_slice, &seqno)) {
    MutexLock l(&mutex_);
    if (manual_garbage_cutoff_ < seqno) {
      manual_garbage_cutoff_ = seqno;
    }
  }
}

bool DBImpl::ValidateTimestamp(const std::string& ts) {
  uint64_t file = 0;
  uint64_t seqno = 0;
  Slice ts_slice(ts);
  return ts == "all" || ts == "now" ||
         (GetVarint64(&ts_slice, &file) &&
          GetVarint64(&ts_slice, &seqno));
}

int DBImpl::CompareTimestamps(const std::string& lhs, const std::string& rhs) {
  uint64_t now = 0;
  uint64_t lhs_seqno = 0;
  uint64_t rhs_seqno = 0;
  uint64_t tmp;
  if (lhs == "now" || rhs == "now") {
    MutexLock l(&mutex_);
    now = versions_->LastSequence();
  }
  if (lhs == "all") {
    lhs_seqno = 0;
  } else if (lhs == "now") {
    lhs_seqno = now;
  } else {
    Slice lhs_slice(lhs);
    GetVarint64(&lhs_slice, &tmp);
    GetVarint64(&lhs_slice, &lhs_seqno);
  }
  if (rhs == "all") {
    rhs_seqno = 0;
  } else if (rhs == "now") {
    rhs_seqno = now;
  } else {
    Slice rhs_slice(rhs);
    GetVarint64(&rhs_slice, &tmp);
    GetVarint64(&rhs_slice, &rhs_seqno);
  }

  if (lhs_seqno < rhs_seqno) {
    return -1;
  } else if (lhs_seqno > rhs_seqno) {
    return 1;
  } else {
    return 0;
  }
}

Status DBImpl::GetReplayIterator(const std::string& timestamp,
                                 ReplayIterator** iter) {
  *iter = NULL;
  Slice ts_slice(timestamp);
  uint64_t file = 0;
  uint64_t seqno = 0;

  if (timestamp == "all") {
    seqno = 0;
  } else if (timestamp == "now") {
    {
      MutexLock l(&mutex_);
      file = versions_->NewFileNumber();
      versions_->ReuseFileNumber(file);
      seqno = versions_->LastSequence();
    }
    WaitOutWriters();
  } else if (!GetVarint64(&ts_slice, &file) ||
             !GetVarint64(&ts_slice, &seqno)) {
    return Status::InvalidArgument("Timestamp is not valid");
  }

  ReadOptions options;
  options.fill_cache = false;
  SequenceNumber latest_snapshot;
  uint32_t seed;
  MutexLock l(&mutex_);
  Iterator* internal_iter = NewInternalIterator(options, file, &latest_snapshot, &seed, true);
  internal_iter->SeekToFirst();
  ReplayIteratorImpl* iterimpl;
  iterimpl = new ReplayIteratorImpl(
      this, &mutex_, user_comparator(), internal_iter, mem_, SequenceNumber(seqno));
  *iter = iterimpl;
  replay_iters_.push_back(iterimpl);
  return Status::OK();
}

void DBImpl::ReleaseReplayIterator(ReplayIterator* _iter) {
  MutexLock l(&mutex_);
  ReplayIteratorImpl* iter = reinterpret_cast<ReplayIteratorImpl*>(_iter);
  for (std::list<ReplayIteratorImpl*>::iterator it = replay_iters_.begin();
      it != replay_iters_.end(); ++it) {
    if (*it == iter) {
      iter->cleanup(); // calls delete
      replay_iters_.erase(it);
      return;
    }
  }
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  ++straight_reads_;
  if (versions_->current()->RecordReadSample(key)) {
    bg_compaction_cv_.Signal();
  }
}

SequenceNumber DBImpl::LastSequence() {
  SequenceNumber ret;

  {
    MutexLock l(&mutex_);
    ret = versions_->LastSequence();
  }

  WaitOutWriters();
  return ret;
}

const Snapshot* DBImpl::GetSnapshot() {
  const Snapshot* ret;

  {
    MutexLock l(&mutex_);
    ret = snapshots_.New(versions_->LastSequence());
  }

  WaitOutWriters();
  return ret;
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
#if VLOG
	uint64_t value_address = koo::db->vlog->AddRecord(key, val);
	char buffer[sizeof(uint64_t) + sizeof(uint32_t)];
	EncodeFixed64(buffer, value_address);
	EncodeFixed32(buffer + sizeof(uint64_t), val.size());
	return DB::Put(o, key, (Slice) {buffer, sizeof(uint64_t) + sizeof(uint32_t)});
#else
  return DB::Put(o, key, val);
#endif
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&writers_mutex_);
  Status s;
  s = SequenceWriteBegin(&w, updates);

  if (s.ok() && updates != NULL) { // NULL batch is for compactions
    WriteBatchInternal::SetSequence(updates, w.start_sequence_);

#if !VLOG
    // Add to log and apply to memtable.  We do this without holding the lock
    // because both the log and the memtable are safe for concurrent access.
    // The synchronization with readers occurs with SequenceWriteEnd.
    s = w.log_->AddRecord(WriteBatchInternal::Contents(updates));

    if (s.ok() && options.sync) {
      s = w.logfile_->Sync();
    }
#endif
    if (s.ok()) {
      s = WriteBatchInternal::InsertInto(updates, w.mem_);
    }
  }

  if (!s.ok()) {
    mutex_.Lock();
    RecordBackgroundError(s);
    mutex_.Unlock();
  }

  SequenceWriteEnd(&w);
  return s;
}

Status DBImpl::SequenceWriteBegin(Writer* w, WriteBatch* updates) {
  Status s;

  {
    MutexLock l(&mutex_);
    straight_reads_ = 0;
    bool force = updates == NULL;
    bool enqueue_mem = false;
    w->micros_ = versions_->NumLevelFiles(0);

    while (true) {
      if (!bg_error_.ok()) {
        // Yield previous error
        s = bg_error_;
        break;
      } else if (!force &&
                 (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
        // There is room in current memtable
        // Note that this is a sloppy check.  We can overfill a memtable by the
        // amount of concurrently written data.
        break;
      } else if (imm_ != NULL) {
        // We have filled up the current memtable, but the previous
        // one is still being compacted, so we wait.
        bg_memtable_cv_.Signal();
        bg_fg_cv_.Wait();
      } else {
        // Attempt to switch to a new memtable and trigger compaction of old
        assert(versions_->PrevLogNumber() == 0);
        uint64_t new_log_number = versions_->NewFileNumber();
        ConcurrentWritableFile* lfile = NULL;
        s = env_->NewConcurrentWritableFile(LogFileName(dbname_, new_log_number), &lfile);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
          break;
        }
        logfile_.reset(lfile);
        logfile_number_ = new_log_number;
        log_.reset(new log::Writer(lfile));
        imm_ = mem_;
        w->has_imm_ = true;
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
        force = false;   // Do not force another compaction if have room
        enqueue_mem = true;
        break;
      }
    }

    if (s.ok()) {
      w->log_ = log_;
      w->logfile_ = logfile_;
      w->mem_ = mem_;
      mem_->Ref();
    }

    if (enqueue_mem) {
      for (std::list<ReplayIteratorImpl*>::iterator it = replay_iters_.begin();
          it != replay_iters_.end(); ++it) {
        (*it)->enqueue(mem_, w->start_sequence_);
      }
    }
  }

  if (s.ok()) {
    uint64_t diff = updates ? WriteBatchInternal::Count(updates) : 0;
    uint64_t ticket = 0;
    w->linked_ = true;
    w->next_ = NULL;

    writers_mutex_.Lock();
    if (writers_tail_) {
      writers_tail_->next_ = w;
      w->prev_ = writers_tail_;
    }
    writers_tail_ = w;
    ticket = __sync_add_and_fetch(&writers_upper_, 1 + diff);
    while (w->block_if_backup_in_progress_ &&
           backup_in_progress_.Acquire_Load()) {
      w->wake_me_when_head_ = true;
      w->cv_.Wait();
      w->wake_me_when_head_ = false;
    }
    writers_mutex_.Unlock();
    w->start_sequence_ = ticket - diff;
    w->end_sequence_ = ticket;
  }

  return s;
}

void DBImpl::SequenceWriteEnd(Writer* w) {
  if (!w->linked_) {
    return;
  }

  mutex_.Lock();
  versions_->SetLastSequence(w->end_sequence_);
  mutex_.Unlock();

  writers_mutex_.Lock();
  if (w->prev_) {
    w->prev_->next_ = w->next_;
    if (w->has_imm_) {
      w->prev_->has_imm_ = true;
      w->has_imm_ = false;
    }
  }
  if (w->next_) {
    w->next_->prev_ = w->prev_;
    // if we're the head and we're setting someone else to be the head who wants
    // to be notified when they become the head, signal them.
    if (w->next_->wake_me_when_head_ && !w->prev_) {
      w->next_->cv_.Signal();
    }
  }
  if (writers_tail_ == w) {
    assert(!w->next_);
    writers_tail_ = NULL;
  }
  writers_mutex_.Unlock();

  if (w->has_imm_ && !w->prev_) {
    mutex_.Lock();
    has_imm_.Release_Store(imm_);
    w->has_imm_ = false;
    bg_memtable_cv_.Signal();
    mutex_.Unlock();
  }

  if (w->micros_ > config::kL0_SlowdownWritesTrigger) {
    env_->SleepForMicroseconds(w->micros_ - config::kL0_SlowdownWritesTrigger);
  }
}

void DBImpl::WaitOutWriters() {
  Writer w(&writers_mutex_);
  writers_mutex_.Lock();
  if (writers_tail_) {
    writers_tail_->next_ = &w;
    w.prev_ = writers_tail_;
  }
  writers_tail_ = &w;
  w.linked_ = true;
  w.next_ = NULL;
  while (w.prev_) {
    w.wake_me_when_head_ = true;
    w.cv_.Wait();
  }
  assert(!w.prev_);
  if (w.next_) {
    w.next_->prev_ = NULL;
    // if we're the head and we're setting someone else to be the head who wants
    // to be notified when they become the head, signal them.
    if (w.next_->wake_me_when_head_) {
      w.next_->cv_.Signal();
    }
  }
  if (writers_tail_ == &w) {
    assert(!w.next_);
    writers_tail_ = NULL;
  }
  writers_mutex_.Unlock();

  if (w.has_imm_) {
    mutex_.Lock();
    has_imm_.Release_Store(imm_);
    w.has_imm_ = false;
    bg_memtable_cv_.Signal();
    mutex_.Unlock();
  }
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (unsigned level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

Status DBImpl::LiveBackup(const Slice& _name) {
  Slice name = _name;
  size_t name_sz = 0;

  for (; name_sz < name.size() && name.data()[name_sz] != '\0'; ++name_sz)
      ;

  name = Slice(name.data(), name_sz);
  std::set<uint64_t> live;

  {
    MutexLock l(&writers_mutex_);
    backup_in_progress_.Release_Store(this);
    while (backup_waiter_has_it_) {
      ++backup_waiters_;
      backup_cv_.Wait();
      --backup_waiters_;
    }
    backup_waiter_has_it_ = true;
  }

  Writer w(&writers_mutex_);
  w.block_if_backup_in_progress_ = false;
  SequenceWriteBegin(&w, NULL);

  {
    MutexLock l(&writers_mutex_);
    Writer* p = &w;
    while (p->prev_) {
      p = p->prev_;
    }
    while (p != &w) {
      assert(p);
      p->block_if_backup_in_progress_ = false;
      p->cv_.Signal();
      p = p->next_;
    }
    while (w.prev_) {
      w.wake_me_when_head_ = true;
      w.cv_.Wait();
    }
  }

  {
    MutexLock l(&mutex_);
    versions_->SetLastSequence(w.end_sequence_);
    while (bg_log_occupied_) {
      bg_log_cv_.Wait();
    }
    bg_log_occupied_ = true;
    // note that this logic assumes that DeleteObsoleteFiles never releases
    // mutex_, so that once we release at this brace, we'll guarantee that it
    // will see backup_in_progress_.  If you change DeleteObsoleteFiles to
    // release mutex_, you'll need to add some sort of synchronization in place
    // of this text block.
    versions_->AddLiveFiles(&live);
  }

  Status s;
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  std::string backup_dir = dbname_ + "/backup-" + name.ToString() + "/";

  if (s.ok()) {
    s = env_->CreateDir(backup_dir);
  }

  uint64_t number;
  FileType type;

  for (size_t i = 0; i < filenames.size(); i++) {
    if (!s.ok()) {
      continue;
    }
    if (ParseFileName(filenames[i], &number, &type)) {
      std::string src = dbname_ + "/" + filenames[i];
      std::string target = backup_dir + "/" + filenames[i];
      switch (type) {
        case kLogFile:
        case kDescriptorFile:
        case kCurrentFile:
        case kInfoLogFile:
          s = env_->CopyFile(src, target);
          break;
        case kTableFile:
          // If it's a file referenced by a version, we have logged that version
          // and applied it.  Our MANIFEST will reflect that, and the file
          // number assigned to new files will be greater or equal, ensuring
          // that they aren't overwritten.  Any file not in "live" either exists
          // past the current manifest (output of ongoing compaction) or so far
          // in the past we don't care (we're going to delete it at the end of
          // this backup).  I'd rather play safe than sorry.
          //
          // Under no circumstances should you collapse this to a single
          // LinkFile without the conditional as it has implications for backups
          // that share hardlinks.  Opening an older backup that has files
          // hardlinked with newer backups will overwrite "immutable" files in
          // the newer backups because they aren't in our manifest, and we do an
          // open/write rather than a creat/rename.  We avoid linking these
          // files.
          if (live.find(number) != live.end()) {
            s = env_->LinkFile(src, target);
          }
          break;
        case kTempFile:
        case kDBLockFile:
          break;
        default:
          break;
      }
    }
  }

  {
    MutexLock l(&mutex_);
    if (s.ok() && backup_deferred_delete_) {
      DeleteObsoleteFiles();
    }
    backup_deferred_delete_ = false;
    bg_log_occupied_ = false;
    bg_log_cv_.Signal();
  }

  {
    MutexLock l(&writers_mutex_);
    backup_waiter_has_it_ = false;
    if (backup_waiters_ > 0) {
      backup_in_progress_.Release_Store(this);
      backup_cv_.Signal();
    } else {
      backup_in_progress_.Release_Store(NULL);
    }
  }

  SequenceWriteEnd(&w);
  return s;
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;
#if VLOG
	koo::env = options.env;
#endif
#if LEARN
	koo::file_data = new koo::FileLearnedIndexData();
	koo::initial_time = __rdtsc();
#endif

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    ConcurrentWritableFile* lfile;
    s = options.env->NewConcurrentWritableFile(LogFileName(dbname, new_log_number),
                                               &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_.reset(lfile);
      impl->logfile_number_ = new_log_number;
      impl->log_.reset(new log::Writer(lfile));
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_, &impl->bg_log_cv_, &impl->bg_log_occupied_);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();
#if LEARN
			//impl->versions_->current()->ReadModel();
#endif
      impl->bg_compaction_cv_.Signal();
      impl->bg_memtable_cv_.Signal();
    }
  }
  impl->pending_outputs_.clear();
  impl->allow_background_activity_ = true;
  impl->bg_compaction_cv_.SignalAll();
  impl->bg_memtable_cv_.SignalAll();
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
    impl = NULL;
  }
  if (impl) {
    impl->writers_mutex_.Lock();
    impl->writers_upper_ = impl->versions_->LastSequence();
    impl->writers_mutex_.Unlock();
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
#if VLOG
      if ((ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) || filenames[i].find("vlog") != std::string::npos) {  // Lock file will be deleted at end
#else
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
#endif
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

#if LEARN
Version* DBImpl::GetCurrentVersion() {
	MutexLock l(&mutex_);
	Version* ver = versions_->current();
	ver->Ref();
	return ver;
}

void DBImpl::ReturnCurrentVersion(Version* version) {
	MutexLock l(&mutex_);
	version->Unref();
}
#endif

}  // namespace leveldb
