#pragma once

#include "core/core_workload.h"
#include <unistd.h>
#include "workloadwrapper.h"
#include "koo/koo.h"

namespace ycsbc {

class YCSBRunner {
 public:
   YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
              leveldb::Options options,
              std::string data_dir,
              leveldb::DB* db);
   void run_all();
 private:
  const int num_threads_;
  std::vector<CoreWorkload*> workloads_;
  leveldb::Options options_;
  std::string data_dir_;
  leveldb::DB* db_ = NULL;
};

YCSBRunner::YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
                       leveldb::Options options,
                       std::string data_dir,
                       leveldb::DB* db)
    : num_threads_(num_threads),
      workloads_(workloads),
      options_(options),
      data_dir_(data_dir),
      db_(db) {
}

void YCSBRunner::run_all() {
  /*for (auto& wl : workloads_) {
    WorkloadProxy wp(wl);
    RocksDBClient rocksdb_client(&wp, num_threads_, options_, data_dir_, db_);
    rocksdb_client.run();
  }*/
  int size = workloads_.size();
#if YCSB_WRAPPER
	std::vector<WorkloadProxy> wps;
	std::vector<WorkloadWrapper*> wrappers;
	for (int i=0; i<size; i++) {
		WorkloadProxy wp(workloads_[i]);
		wps.push_back(wp);
		bool is_load = wp.is_load();
		size_t load_num = wp.record_count();
		size_t request_num = wp.operation_count();

		if (is_load) wrappers.push_back(new WorkloadWrapper(&wp, load_num, true));
		else wrappers.push_back(new WorkloadWrapper(&wp, request_num, false));
	}

	if (size == 2) {
		RocksDBClient rocksdb_client0(&(wps[0]), num_threads_, options_, data_dir_, db_, wrappers[0]);
		RocksDBClient rocksdb_client1(&(wps[1]), num_threads_, options_, data_dir_, db_, wrappers[1]);
	  rocksdb_client0.run();
#if AC_TEST
		koo::count_compaction_triggered_after_load = true;
#endif
		rocksdb_client1.run();
	} else {
		for (int i=0; i<size; i++) {
	    WorkloadProxy wp(workloads_[i]);
			RocksDBClient rocksdb_client(&(wps[i]), num_threads_, options_, data_dir_, db_, wrappers[i]);
		  rocksdb_client.run();
		}
	}
#else
  for (int i=0; i<size; i++) {
    WorkloadProxy wp(workloads_[i]);
    RocksDBClient rocksdb_client(&wp, num_threads_, options_, data_dir_, db_);
    rocksdb_client.run();

    /*if (i == size-1) continue;
	  printf("Waiting...\n");			// YCSB_CXX
		sleep(5);*/
  }
#endif
}

}  // namespace ycsbc
