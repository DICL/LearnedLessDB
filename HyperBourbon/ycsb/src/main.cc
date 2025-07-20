#include "hyperleveldb/options.h"
#include "src/rocksdb_client.h" 
#include "src/config.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "src/ycsb_runner.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "koo/koo.h"

int main(int argc, char* argv[]){
	utils::Properties common_props;
  std::vector<char> wl_chars;
  parse_command_line_arguments(argc, argv, &common_props, &wl_chars);
#if YCSB_COPYDB
	bool copy_db = true;
#endif

  // Workload
  std::vector<CoreWorkload*> workloads;
  for (auto& wl_char : wl_chars) {
    auto wl = new CoreWorkload();
    if (wl_char == 'l') {
      auto wl_props = gen_workload_props('a', common_props);
      wl->Init(wl_props, /*is_load=*/true);
#if YCSB_COPYDB
			copy_db = false;
#endif
    } else {
      auto wl_props = gen_workload_props(wl_char, common_props);
      wl->Init(wl_props, /*is_load=*/false);
    }
    workloads.push_back(wl);
  }

  // dbpath
  std::string dbpath("/mnt-koo");				// NVMe SSD
  std::string data_dir = dbpath + "/db";

  // db options
	leveldb::Options options;
  options.create_if_missing = true;
  //options.filter_policy = nullptr;

#if MULTI_COMPACTION
  options.num_background_jobs = std::stoi(common_props.GetProperty("max_background_jobs"));
#endif
#if MULTI_LEARNING
  options.env->num_learning_jobs = std::stoi(common_props.GetProperty("max_learning_jobs"));
#endif

#if YCSB_COPYDB
  // Copy DB
	if (copy_db) {
	  fprintf(stdout, "%s: Copy DB start\n", GetDayTime().c_str());
		std::string data_dir_mix = data_dir + "_mix";
	  std::string remove_command = "rm -rf " + data_dir_mix;
		std::string copy_command = "cp -r " + data_dir + " " + data_dir_mix;
	  int rc;
		rc = system(remove_command.c_str());
	  rc = system(copy_command.c_str());
		data_dir = data_dir_mix;
	  fprintf(stdout, "%s: Copy DB finished\n", GetDayTime().c_str());
	}
#endif

  // Open DB
  leveldb::DB* db = nullptr;
#if !YCSB_COPYDB		// workload loading 후 DB Open
  leveldb::Status s = leveldb::DB::Open(options, data_dir, &db);
  std::cout << "DB Opened main.cc\n";
#endif

  // Init and Run Workloads
  int num_threads = std::stoi(common_props.GetProperty("threadcount"));
  YCSBRunner runner(num_threads, workloads, options, data_dir, db);
#if YCSB_COPYDB
  db = runner.run_all();
  if (db) {
  	fprintf(stderr, "Error deleting db\n");
  	delete db;
	}
#else
  runner.run_all();

	fprintf(stdout, "Start deleting db: %s\n", GetDayTime().c_str());
	fflush(stdout);
	delete db;
	fprintf(stdout, "Finish deleting db: %s\n", GetDayTime().c_str());
	fflush(stdout);
#endif

	for (auto& wl : workloads) delete wl;

	fflush(stdout);
	return 0;
}
