//#include "rocksdb/options.h"
//#include "/koo/Bourbon/include/rocksdb/options.h"
#include "hyperleveldb/options.h"
#include "src/rocksdb_client.h" 
#include "src/config.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "src/ycsb_runner.h"
#include "db/db_impl.h"
#include "db/version_set.h"

int main(int argc, char* argv[]){
	utils::Properties common_props;
  std::vector<char> wl_chars;
  parse_command_line_arguments(argc, argv, &common_props, &wl_chars);

  // Workload
  std::vector<CoreWorkload*> workloads;
  for (auto& wl_char : wl_chars) {
    auto wl = new CoreWorkload();
    if (wl_char == 'l') {
			//for (const auto& entry : std::experimental::filesystem::directory_iterator("/mnt-koo/"))			// KOO
				//std::experimental::filesystem::remove_all(entry.path());
      auto wl_props = gen_workload_props('a', common_props);
      wl->Init(wl_props, /*is_load=*/true);
    } else {
      auto wl_props = gen_workload_props(wl_char, common_props);
      wl->Init(wl_props, /*is_load=*/false);
    }
    workloads.push_back(wl);
  }

  //for (const auto& entry : std::experimental::filesystem::directory_iterator("/koo/HyperLevelDB-Learningless/koo/data/"))			// KOO
  	//std::experimental::filesystem::remove_all(entry.path());

  // dbpath
  //char user_name[100] = "koo";
  //strcpy(user_name, "koo", 3);
  //getlogin_r(user_name, 100);
  //std::string dbpath("pmem/rocksdb_");
  //std::string dbpath("/koo/ycsb_data");		// SATA Disk
  std::string dbpath("/mnt-koo");				// NVMe SSD
  //dbpath.append(user_name);

  // db options
	//rocksdb::Options options;
	leveldb::Options options;
  //options.wal_dir = dbpath + "/wal";
  options.create_if_missing = true;
  //options.max_write_buffer_number = std::stoi(common_props.GetProperty("max_write_buffer_number"));
  //options.max_background_jobs = std::stoi(common_props.GetProperty("max_background_jobs"));
  //options.filter_policy = nullptr;

  // Open DB
  std::string data_dir = dbpath + "/db";
  //rocksdb::DB* db;
  //rocksdb::Status s = rocksdb::DB::Open(options, data_dir, &db);
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(options, data_dir, &db);
  std::cout << "DB Open\n";

//std::string pout;
//db->GetProperty("rocksdb.block-cache-capacity", &pout);
//std::cerr << pout << std::endl;
////fprintf(stderr, "block_cache_size=%zu\n", options.block-cache-size);
//exit(0);

  // Init and Run Workloads
  int num_threads = std::stoi(common_props.GetProperty("threadcount"));
  YCSBRunner runner(num_threads, workloads, options, data_dir, db);
  runner.run_all();

	delete db;

	for (auto& wl : workloads) delete wl;

	fflush(stdout);
	return 0;
}
