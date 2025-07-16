
#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fstream>
#include <cmath>
#include <random>
#include <sys/time.h>
#include <thread>
#include "hyperleveldb/db.h"
#include "hyperleveldb/comparator.h"
#include "koo/util.h"
#include "koo/learned_index.h"
#include "koo/cxxopts.hpp"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "koo/koo.h"
#include <sstream>
#include <algorithm>
#include <random>

using namespace leveldb;
using std::string;
using std::cout;
using std::endl;
using std::to_string;
using std::vector;
using std::map;
using std::ifstream;
using std::ofstream;
using std::string;
using std::thread;

#define TIME_NOW (std::chrono::system_clock::now())
typedef std::chrono::system_clock::time_point TimePoint;
typedef std::chrono::nanoseconds NanoSec;
size_t KEY_SIZE = 16;
size_t VALUE_SIZE = 64;

static std::string GetDayTime(){
		const int kBufferSize = 100;
		char buffer[kBufferSize];
		struct timeval now_tv;
		gettimeofday(&now_tv, nullptr);
		const time_t seconds = now_tv.tv_sec;
		struct tm t;
		localtime_r(&seconds, &t);
		snprintf(buffer, kBufferSize,
							"%04d/%02d/%02d-%02d:%02d:%02d.%06d",
		          t.tm_year + 1900,
		          t.tm_mon + 1,
		          t.tm_mday,
		          t.tm_hour,
		          t.tm_min,
	            t.tm_sec,
		          static_cast<int>(now_tv.tv_usec));
		return std::string(buffer);
}

enum OpType {
	OP_INSERT = 0,
	OP_UPDATE = 1,
	OP_READ = 2,
	OP_SCAN = 3,
	OP_READMODIFYWRITE = 4
};

string EncodeUint64To16ByteString(uint64_t value) {
	string result(16, '\0');
	for (int i=0; i<8; ++i) {
		result[8 + i] = static_cast<char>((value >> (56 - i * 8)) & 0xFF);
	}
	return std::move(result);
}

string generate_key(uint64_t& data) {
	std::ostringstream ss;
	ss << data;
	string key = ss.str();
	size_t size = key.length();
	string result;
	if (size <= KEY_SIZE) {
		result = string(KEY_SIZE - size, '0') + key;
	} else {
		result = key.substr(0, KEY_SIZE);
		//result = key.substr(size - KEY_SIZE, KEY_SIZE);
	}
	return std::move(result);
}

vector<string> load_data(std::string filename) {
	vector<uint64_t> data;
	ifstream in("/koo/SOSD/data/"+filename, std::ios::binary);
	if (!in.is_open()) {
		fprintf(stderr, "unable to open %s\n", filename.c_str());
		exit(1);
	}
	uint64_t size;		// read size
	in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
	data.resize(size);
	in.read(reinterpret_cast<char*>(data.data()), size * sizeof(uint64_t));
	in.close();

	vector<string> data_str;
	data_str.reserve(size);
	for (uint64_t d : data) {
		string str = EncodeUint64To16ByteString(d);
		//string str = generate_key(d);
		data_str.push_back(std::move(str));
		//std::cout << d << " " << str << " " << data_str.back() << std::endl;
	}

	return data_str;
}

int main(int argc, char *argv[]) {
  int rc;
  string db_location = "/mnt-koo/db";
  string data_filename = "";
  string lookups_filename = "", ops_filename = "";
  bool evict = false, random_order = true;
  int num_threads = 1;

  for (int i=1; i<argc; i++) {
    int n;
  	char junk;
  	if (Slice(argv[i]).starts_with("--data_file=")) {
			char* input_filename = argv[i] + strlen("--data_file=");
			data_filename.assign(input_filename);
  	} else if (Slice(argv[i]).starts_with("--lookups_file=")) {
			char* input_filename = argv[i] + strlen("--lookups_file=");
			lookups_filename.assign(input_filename);
  	} else if (Slice(argv[i]).starts_with("--ops_file=")) {
			char* input_filename = argv[i] + strlen("--ops_file=");
			ops_filename.assign(input_filename);
		} else if (sscanf(argv[i], "--num_threads=%d%c", &n, &junk) == 1) {
			num_threads = n;
		} else if (sscanf(argv[i], "--evict=%d%c", &n, &junk) == 1) {
			evict = n;
		} else if (sscanf(argv[i], "--random_order=%d%c", &n, &junk) == 1) {
			random_order = n;
		} else {
			fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
			exit(1);
		}
	}
	if (data_filename == "" && lookups_filename == "") { 
		fprintf(stderr, "Need filename\n"); exit(1); 
	}
	if (data_filename == "" && lookups_filename != "") { 
		fprintf(stdout, "Only lookups_file. DB should be loaded previously\n");
	}
	if (lookups_filename != "" && ops_filename == "") { 
		fprintf(stdout, "No ops_file. Read-only workload\n");
		fflush(stdout);
	}

	// Start
	DB* db = nullptr;
  Options options;
  ReadOptions read_options = ReadOptions();
  WriteOptions write_options = WriteOptions();
  Status status;
  options.create_if_missing = true;
  write_options.sync = false;
  static char w_value_buf[4096];
  for (int i=0; i<4096; i++) w_value_buf[i] = (char)i;
  Slice w_value(w_value_buf, VALUE_SIZE);

  // Prepare keys and lookups
  vector<string> keys;
  if (data_filename != "") {
  	keys = load_data(data_filename);
		if (random_order) {
			unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
			std::mt19937 generator(seed);
			std::shuffle(keys.begin(), keys.end(), generator);
		}
	}
	size_t load_size = keys.size();
  vector<string> lookups;
  if (lookups_filename != "") lookups = load_data("lookups/"+lookups_filename);
	size_t run_size = lookups.size();
	/*std::ofstream ofs("/koo/SOSD/data/data/" + data_filename + "_random");
	for (auto d : keys) ofs << d << std::endl;
	ofs.close();*/

	// Load
	if (data_filename != "") {
    // Clear existing directory, clear page cache, trim SSD
    string command = "rm -rf " + db_location;
    rc = system(command.c_str());
		if (evict) {
			rc = system("sync; echo 1 | sudo tee /proc/sys/vm/drop_caches");
			fprintf(stdout, "%s: Drop cache\n", GetDayTime().c_str());
		}

		// Perform load
		size_t part = load_size / num_threads;
		fprintf(stdout, "\n#########################################################################\n");
		fprintf(stdout, "%s: Load DB (%lu kvs, %d clients)\n", GetDayTime().c_str(), load_size, num_threads);
		fflush(stdout);
    status = DB::Open(options, db_location, &db);
    assert(status.ok() && "Open Error");
    thread ths[num_threads];
    auto t_load = [&](size_t start, size_t len, int tid) {
      Status s;
      auto end = start + len;
      for (size_t i=start; i<end; i++) {
      	//std::cout << keys[i] << " " << keys[i].length() << std::endl;
        s = db->Put(write_options, keys[i], w_value);
        assert(s.ok() && "File Put Error");
			}
		};
		TimePoint start_time = TIME_NOW;
		for (size_t i=0; i<num_threads; i++) {
			ths[i] = thread(t_load, part * i, part, i);
		}
		for (size_t i=0; i<num_threads; i++) {
			ths[i].join();
		}
		NanoSec wl_time = TIME_NOW - start_time;
		fprintf(stdout, "Throughput: %f KOPS\n", (load_size/1000.0) / (wl_time.count()/(1000.0*1000.0*1000.0)));
		fprintf(stdout, "%s: Load complete\n", GetDayTime().c_str());
		koo::Report();
		koo::Reset();
		fflush(stdout);

		if (lookups_filename == "") {
			delete db;
			db = nullptr;
			fprintf(stdout, "%s: Finish deleting db\n", GetDayTime().c_str());
		}
	}

	// Workloads
	if (lookups_filename != "") {
		if (data_filename == "") {
			// Copy DB
			fprintf(stdout, "\n#########################################################################\n");
			fprintf(stdout, "%s: Copy DB Start\n", GetDayTime().c_str());
			string db_location_mix = db_location + "_mix";
			string remove_command = "rm -rf " + db_location_mix;
			string copy_command = "cp -r " + db_location + " " + db_location_mix;
			rc = system(remove_command.c_str());
			rc = system(copy_command.c_str());
			fprintf(stdout, "%s: Copy DB Finished\n", GetDayTime().c_str());
			if (evict) {
				rc = system("sync; echo 1 | sudo tee /proc/sys/vm/drop_caches");
				fprintf(stdout, "%s: Drop cache\n", GetDayTime().c_str());
			}

			status = DB::Open(options, db_location_mix, &db);
	    assert(status.ok() && "Open Error");
		}

		// Run workloads
		size_t part = run_size / num_threads;
		fprintf(stdout, "\n#########################################################################\n");
		fprintf(stdout, "%s: Starting up (%lu ops, %d clients)\n", GetDayTime().c_str(), run_size, num_threads);
		fflush(stdout);
		//bool start_new_event = true;
		thread ths[num_threads];
		auto t_operate = [&](size_t start, size_t len, int tid) {
			Status s;
			auto end = start + len;
			for (size_t i=start; i<end; i++) {
				string r_value;
				s = db->Get(read_options, lookups[i], &r_value);
				if (!s.ok()) {
					uint64_t num = 0;
					for (int j=0; j<8; j++) {
						num |= static_cast<uint64_t>(static_cast<unsigned char>(lookups[i].data()[8 + j])) << (56 - j * 8);
					}
					fprintf(stdout, "%s key: %lu, file: %s\n", s.ToString().c_str(), num, lookups_filename.c_str());
				}
			}
		};
		TimePoint start_time = TIME_NOW;
		for (size_t i=0; i<num_threads; i++) {
			ths[i] = thread(t_operate, part * i, part, i);
		}
		for (size_t i=0; i<num_threads; i++) {
			ths[i].join();
		}
		NanoSec wl_time = TIME_NOW - start_time;
		fprintf(stdout, "Throughput: %f KOPS\n", (run_size/1000.0) / (wl_time.count()/(1000.0*1000.0*1000.0)));
		fprintf(stdout, "%s: Finished\n\n", GetDayTime().c_str());
		koo::Report();
		koo::Reset();
		fflush(stdout);

		delete db;
		db = nullptr;
		fprintf(stdout, "%s: Finish deleting db\n", GetDayTime().c_str());
	}

  // report various data after the run
  // print out averages
  // TODO




			/*thread ths[num_threads];
			auto t_operate = [&](size_t start, size_t len, int tid) {
				Status s;
				auto end = start + len;
				for (size_t i=start; i<end; i++) {
					int op = ops[wl][i];
					if (op == OP_INSERT || op == OP_UPDATE) {
						s = db->Put(write_options, keys[wl][i], w_value);
						assert(s.ok() && "Key Put Error");
						//if (!s.ok()) fprintf(stderr, "Key %s Put Error\n", keys[wl][i].c_str());
					} else if (op == OP_READ) {
						string r_value;
						s = db->Get(read_options, keys[wl][i], &r_value);
						assert(s.ok() && "Key Not Found");
						//if (!s.ok()) fprintf(stderr, "Key %s Not Found\n", keys[wl][i].c_str());
					} else if (op == OP_READMODIFYWRITE) {
						string r_value;
						s = db->Get(read_options, keys[wl][i], &r_value);
						assert(s.ok() && "Key Not Found");
						s = db->Put(write_options, keys[wl][i], w_value);
						assert(s.ok() && "Key Put Error");
					} else if (op == OP_SCAN) {
						fprintf(stderr, "TODO"); continue;
					} else { fprintf(stdout, "Unsupported operation\n"); exit(1); }
				}
			};
			TimePoint start_time = TIME_NOW;
			for (size_t i=0; i<num_threads; i++) {
				ths[i] = thread(t_operate, part * i, part, i);
			}
			for (size_t i=0; i<num_threads; i++) {
				ths[i].join();
			}*/



  return 0;
}
