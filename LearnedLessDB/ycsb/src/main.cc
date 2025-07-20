//#include "rocksdb/options.h"
#include "hyperleveldb/options.h"
#include "src/rocksdb_client.h" 
#include "src/config.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "src/ycsb_runner.h"
#include "koo/koo.h"
//#include <experimental/filesystem>

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

  // db options
	leveldb::Options options;
  options.create_if_missing = true;
  //options.filter_policy = nullptr;

  options.num_background_jobs = std::stoi(common_props.GetProperty("max_background_jobs"));
	koo::learn_model_error = std::stod(common_props.GetProperty("learned_model_error_bound"));
	koo::merge_model_error = std::stod(common_props.GetProperty("merged_model_error_bound"));
	std::string data_dir = common_props.GetProperty("db_path");

  // Copy DB
#if YCSB_COPYDB
	if (copy_db) {
		std::string data_dir_mix = data_dir + "_mix";
	  std::string remove_command = "rm -rf " + data_dir_mix;
		std::string copy_command = "cp -r " + data_dir + " " + data_dir_mix;
	  int rc;
		rc = system(remove_command.c_str());
	  rc = system(copy_command.c_str());
		data_dir = data_dir_mix;
	}
#endif

  // Open DB
  leveldb::DB* db = nullptr;
#if !YCSB_COPYDB				// workload loading 후 DB Open
  leveldb::Status s = leveldb::DB::Open(options, data_dir, &db);
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
	delete db;
#endif

	for (auto& wl : workloads) delete wl;

	fflush(stdout);
	return 0;
}
