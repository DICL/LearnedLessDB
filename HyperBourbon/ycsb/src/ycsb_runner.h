#pragma once

#include "core/core_workload.h"
#include <unistd.h>
#include "workloadwrapper.h"
#include "koo/koo.h"
#include "hyperleveldb/db.h"
#include "hyperleveldb/options.h"
#include "hyperleveldb/status.h"

namespace ycsbc {

class YCSBRunner {
 public:
   YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
              leveldb::Options options,
              std::string data_dir,
              leveldb::DB* db);
#if YCSB_COPYDB
   leveldb::DB* run_all();
#else
   void run_all();
#endif
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

#if YCSB_COPYDB
leveldb::DB* YCSBRunner::run_all() {
#else
void YCSBRunner::run_all() {
#endif
  /*for (auto& wl : workloads_) {
    WorkloadProxy wp(wl);
    RocksDBClient rocksdb_client(&wp, num_threads_, options_, data_dir_, db_);
    rocksdb_client.run();
  }*/
  int size = workloads_.size();
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

#if YCSB_COPYDB
	if (!db_) {
		leveldb::Status s = leveldb::DB::Open(options_, data_dir_, &db_);
		if (!s.ok() || !db_) {
			fprintf(stderr, "%s\n", s.ToString().c_str());
			exit(0);
		}
		fprintf(stdout, "DB Opened ycsb_runner.h\n");
	}
#endif

	if (size == 2) {		// Load A -> A/B/C...
		RocksDBClient rocksdb_client0(&(wps[0]), num_threads_, options_, data_dir_, db_, wrappers[0]);
		RocksDBClient rocksdb_client1(&(wps[1]), num_threads_, options_, data_dir_, db_, wrappers[1]);
		rocksdb_client0.run();
		rocksdb_client1.run();
#if YCSB_COPYDB
		fprintf(stdout, "Start deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
		delete db_;
		db_ = nullptr;
		fprintf(stdout, "Finish deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
#endif
	} else if (size == 6) {		// Load A -> A -> B -> C -> F -> D
		RocksDBClient rocksdb_client0(&(wps[0]), num_threads_, options_, data_dir_, db_, wrappers[0]);
		RocksDBClient rocksdb_client1(&(wps[1]), num_threads_, options_, data_dir_, db_, wrappers[1]);
		RocksDBClient rocksdb_client2(&(wps[2]), num_threads_, options_, data_dir_, db_, wrappers[2]);
		RocksDBClient rocksdb_client3(&(wps[3]), num_threads_, options_, data_dir_, db_, wrappers[3]);
		RocksDBClient rocksdb_client4(&(wps[4]), num_threads_, options_, data_dir_, db_, wrappers[4]);
		RocksDBClient rocksdb_client5(&(wps[5]), num_threads_, options_, data_dir_, db_, wrappers[5]);
		rocksdb_client0.run();
		rocksdb_client1.run();
		rocksdb_client2.run();
		rocksdb_client3.run();
		rocksdb_client4.run();
		rocksdb_client5.run();
#if YCSB_COPYDB
		fprintf(stdout, "Start deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
		delete db_;
		db_ = nullptr;
		fprintf(stdout, "Finish deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
#endif
	} else if (size == 1) {
		RocksDBClient rocksdb_client(&(wps[0]), num_threads_, options_, data_dir_, db_, wrappers[0]);
		rocksdb_client.run();
#if YCSB_COPYDB
		fprintf(stdout, "Start deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
		delete db_;
		db_ = nullptr;
		fprintf(stdout, "Finish deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
#endif
	} else {
		for (int i=0; i<size; i++) {
			WorkloadProxy wp(workloads_[i]);
			RocksDBClient rocksdb_client(&(wps[i]), num_threads_, options_, data_dir_, db_, wrappers[i]);
			rocksdb_client.run();
		}
#if YCSB_COPYDB
		fprintf(stdout, "Start deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
		delete db_;
		db_ = nullptr;
		fprintf(stdout, "Finish deleting db: %s\n", GetDayTime().c_str());
		fflush(stdout);
#endif
	}
#if YCSB_COPYDB
	return db_;
#endif
  /*for (int i=0; i<size; i++) {
    WorkloadProxy wp(workloads_[i]);
    RocksDBClient rocksdb_client(&wp, num_threads_, options_, data_dir_, db_);
    rocksdb_client.run();
  }*/

}
}  // namespace ycsbc
