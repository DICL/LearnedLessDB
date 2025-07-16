
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
size_t VALUE_SIZE = 8;

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

string generate_key(const string& key) {
	assert(KEY_SIZE >= key.length());
	string result = string(KEY_SIZE - key.length(), '0') + key;
	return std::move(result);
}

enum OpType {
	OP_INSERT = 0,
	OP_UPDATE = 1,
	OP_READ = 2,
	OP_SCAN = 3,
	OP_READMODIFYWRITE = 4
};

int main(int argc, char *argv[]) {
    int rc;
    string db_location = "/mnt-koo/db";
    bool evict = false, fresh_write = false;
    char* input_filename = nullptr;
    int num_threads = 1, num_workloads = 0;

    for (int i=1; i<argc; i++) {
    	int n;
    	char junk;
    	if (Slice(argv[i]).starts_with("--workloads=")) {
    		input_filename = argv[i] + strlen("--workloads=");
			} else if (sscanf(argv[i], "--num_workloads=%d%c", &n, &junk) == 1) {
				num_workloads = n;
			} else if (sscanf(argv[i], "--num_threads=%d%c", &n, &junk) == 1) {
				num_threads = n;
			} else if (sscanf(argv[i], "--evict=%d%c", &n, &junk) == 1) {
				evict = n;
			} else if (sscanf(argv[i], "--fresh_write=%d%c", &n, &junk) == 1) {
				fresh_write = n;
			} else {
				fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
				exit(1);
			}
		}
    if (!num_workloads) { fprintf(stderr, "Need num_workloads\n"); exit(1); }
		if (!input_filename) { fprintf(stderr, "Need workloads\n"); exit(1); }

    vector<string> keys[num_workloads];
    vector<int> ops[num_workloads];
    int num_wl = 0;
    while (input_filename != nullptr) {
    	char* sep = strchr(input_filename, ',');
    	string fn;
    	if (sep == nullptr) {
    		fn = input_filename;
    		input_filename = nullptr;
			} else {
				fn = string(input_filename, sep - input_filename);
				input_filename = sep + 1;
			}
			fprintf(stdout, "%s: Reading file %s\n", GetDayTime().c_str(), fn.c_str());
			fflush(stdout);
			ifstream input(fn);
	    string op, key;
		  while (input.good() && input >> op >> key) {
		  	string the_key = generate_key(key);
		  	keys[num_wl].push_back(std::move(the_key));
		  	if (op.compare("INSERT") == 0) {
		  		ops[num_wl].push_back(OP_INSERT);
				} else if (op.compare("UPDATE") == 0) {
		  		ops[num_wl].push_back(OP_UPDATE);
				} else if (op.compare("READ") == 0) {
		  		ops[num_wl].push_back(OP_READ);
				} else if (op.compare("SCAN") == 0) {
					ops[num_wl].push_back(OP_SCAN);
				//} else if (op.compare("4") == 0) {
					//ops[num_wl].push_back(OP_READMODIFYWRITE);
				} else { fprintf(stderr, "Unsupported operation: %s\n", op.c_str()); exit(1); }
		  }
		  num_wl++;
			input.close();
		}
		if (num_wl != num_workloads) { fprintf(stderr, "num_wl != num_workloads\n"); exit(1); }

		// Start
    DB* db;
    Options options;
    ReadOptions read_options = ReadOptions();
    WriteOptions write_options = WriteOptions();
    Status status;
    options.create_if_missing = true;
    write_options.sync = false;
    static char w_value_buf[4096];
    for (int i=0; i<4096; i++) w_value_buf[i] = (char)i;
    Slice w_value(w_value_buf, VALUE_SIZE);

		for (int wl=0; wl<num_workloads; wl++) {
	    if (wl == 0 && fresh_write) {		// Load DB
	    	size_t load_size = keys[0].size();
        size_t part = load_size / num_threads;
				fprintf(stdout, "%s: Load DB (wl%d, %lu kvs, %d clients)\n", GetDayTime().c_str(), wl, load_size, num_threads);
				fflush(stdout);

        // clear existing directory, clear page cache, trim SSD
        string command = "rm -rf " + db_location;
        rc = system(command.c_str());
        status = DB::Open(options, db_location, &db);
        assert(status.ok() && "Open Error");

        // perform load
        thread ths[num_threads];
        auto t_load = [&](size_t start, size_t len, int tid) {
        	Status s;
        	auto end = start + len;
        	for (size_t i=start; i<end; i++) {
        		s = db->Put(write_options, keys[0][i], w_value);
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

        // reopen DB and do offline leraning
         /*if (print_file_info) db->PrintFileInfo();
         adgMod::db->WaitForBackground();
        delete db;
         status = DB::Open(options, db_location, &db);
         adgMod::db->WaitForBackground();
        Version* current = adgMod::db->versions_->current();
				// offline file learning
        current->FileLearn();
         cout << "Shutting down" << endl;
        adgMod::db->WaitForBackground();
         delete db;*/
        
#if YCSB_DB
				fprintf(stdout, "\n#########################################################################\n");
				string stat_str;
				db->GetProperty("leveldb.stats", &stat_str);
				printf("\n%s\n", stat_str.c_str());
				fprintf(stdout, "%s: Before WaitForBackground()\n\n", GetDayTime().c_str());
				fflush(stdout);
				db->WaitForBackground();
				fprintf(stdout, "%s: After WaitForBackground()\n\n", GetDayTime().c_str());
				string stat_str2;
				db->GetProperty("leveldb.stats", &stat_str2);
				printf("\n%s\n", stat_str2.c_str());
				koo::Report();
				koo::Reset();
				fflush(stdout);
#endif
				continue;
			}

			// Copy DB
			//if (!fresh_write) {
			if (!fresh_write && wl == 0) {			// 처음 한번만 copy db
				/*if (wl > 0) {
					db->WaitForBackground();
					string stat_str;
					db->GetProperty("leveldb.stats", &stat_str);
					printf("\n%s\n", stat_str.c_str());
					delete db;
				}*/
				fprintf(stdout, "%s: Copy DB Start\n", GetDayTime().c_str());
				string db_location_mix = db_location + "_mix";
				string remove_command = "rm -rf " + db_location_mix;
				string copy_command = "cp -r " + db_location + " " + db_location_mix;

				rc = system(remove_command.c_str());
				rc = system(copy_command.c_str());
				//db_location = db_location_mix;
				fprintf(stdout, "%s: Copy DB Finished\n", GetDayTime().c_str());

				if (evict) {
					rc = system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
					fprintf(stdout, "%s: Drop cache\n", GetDayTime().c_str());
				}
				status = DB::Open(options, db_location_mix, &db);
				assert(status.ok() && "Open Error");
			}

			/*if (evict && wl > 0) {
				db->WaitForBackground();
				delete db;
				rc = system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
				fprintf(stdout, "%s: Drop cache\n", GetDayTime().c_str());
				status = DB::Open(options, db_location, &db);
				assert(status.ok() && "Open Error");
			}
	    (void) rc;*/

			size_t run_size = keys[wl].size();
			size_t part = run_size / num_threads;
			fprintf(stdout, "\n#########################################################################\n");
			fprintf(stdout, "%s: Starting up (wl%d, %lu ops, %d clients)\n", GetDayTime().c_str(), wl, run_size, num_threads);
			fflush(stdout);
	    bool start_new_event = true;
		  /*if (fresh_write && wl == 0) {
				status = DB::Open(options, db_location, &db);
				assert(status.ok() && "Open Error");
			}*/

			thread ths[num_threads];
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
			}
			NanoSec wl_time = TIME_NOW - start_time;
			fprintf(stdout, "Throughput: %f KOPS\n", (run_size/1000.0) / (wl_time.count()/(1000.0*1000.0*1000.0)));
			fprintf(stdout, "%s: Finished\n\n", GetDayTime().c_str());
			koo::Report();
			koo::Reset();
			fflush(stdout);
		}

    // report various data after the run
#if YCSB_DB
    if (fresh_write && num_workloads == 1) {
			string stat_str;
			db->GetProperty("leveldb.stats", &stat_str);
			printf("\n%s\n", stat_str.c_str());
			fprintf(stdout, "%s: Before WaitForBackground()\n\n", GetDayTime().c_str());
			fflush(stdout);
			db->WaitForBackground();
			fprintf(stdout, "%s: After WaitForBackground()\n\n", GetDayTime().c_str());
		}
#endif
		/*string stat_str2;
		db->GetProperty("leveldb.stats", &stat_str2);
		printf("\n%s\n", stat_str2.c_str());
		fflush(stdout);*/
    delete db;
    fprintf(stdout, "%s: Finish deleting db\n", GetDayTime().c_str());

    // print out averages
    // TODO

    return 0;
}
