
#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fstream>
#include <cmath>
#include <random>
#include <sys/time.h>
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

int mix_base = 20;
int key_size = 16;
size_t MAX_NUM_WORKLOAD = 5;		// A~F

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
	assert(key_size >= key.length());
	string result = string(key_size - key.length(), '0') + key;
	return std::move(result);
}

enum OpType {
	OP_INSERT = 0,
	OP_UPDATE = 1,
	OP_READ = 2,
	OP_SCAN = 3
};

int main(int argc, char *argv[]) {
    int rc;
    int num_operations;
    string db_location;
    char* input_filename;
    //bool print_single_timing, print_file_info;
    bool evict, fresh_write;
    int insert_bound;
    int value_size, num_threads;

    cxxopts::Options commandline_options("leveldb read test", "Testing leveldb read performance.");
    commandline_options.add_options()
            //("n,get_number", "the number of gets", cxxopts::value<int>(num_operations)->default_value("10000000"))
            ("d,directory", "the directory of db", cxxopts::value<string>(db_location)->default_value("/mnt-koo/db/"))
            ("k,key_size", "the size of key", cxxopts::value<int>(key_size)->default_value("16"))
            ("v,value_size", "the size of value", cxxopts::value<int>(value_size)->default_value("8"))
            //("single_timing", "print the time of every single get", cxxopts::value<bool>(print_single_timing)->default_value("false"))
            //("file_info", "print the file structure info", cxxopts::value<bool>(print_file_info)->default_value("false"))
            ("f,input_file", "the filename of input file", cxxopts::value<char*>(input_filename)->default_value(nullptr))
            ("w,write", "writedb", cxxopts::value<bool>(fresh_write)->default_value("true"))
            ("c,uncache", "evict cache", cxxopts::value<bool>(evict)->default_value("false"))
            //("filter", "use filter", cxxopts::value<bool>(adgMod::use_filter)->default_value("false"))
            ("insert", "insert new value", cxxopts::value<int>(insert_bound)->default_value("0"))
            ("threads", "the number of clients", cxxopts::value<int>(num_threads)->default_value("1"));
    auto result = commandline_options.parse(argc, argv);
		if (!input_filename) { fprintf(stderr, "Need input_filename\n"); exit(1); }

    std::default_random_engine e1(0), e2(255), e3(0);
    srand(0);

    vector<string> keys[MAX_NUM_WORKLOAD];
    vector<int> ops[MAX_NUM_WORKLOAD];
		size_t num_wl = 0;
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
			ifstream input(input_filename);
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
				} else continue;
		  }
		  num_wl++;
		}

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
    Slice w_value(w_value_buf, value_size);

		for (int wl=0; wl<num_wl; wl++) {
	    if (wl == 0 && fresh_write) {
	    	size_t load_size = keys[wl].size();
				printf("%s: Load DB (%lu kvs)\n", GetDayTime().c_str(), load_size);
        // Load DB
        // clear existing directory, clear page cache, trim SSD
        string command = "rm -rf " + db_location;
        rc = system(command.c_str());
        status = DB::Open(options, db_location, &db);
        assert(status.ok() && "Open Error");

        // perform load
        for (int i=0; i<load_size; i++) {
          status = db->Put(write_options, keys[wl][i], w_value);
					assert(status.ok() && "File Put Error");
        }
				printf("%s: Load complete\n", GetDayTime().c_str());
				continue;

        // reopen DB and do offline leraning
         /*if (print_file_info) db->PrintFileInfo();
         adgMod::db->WaitForBackground();
        delete db;
         status = DB::Open(options, db_location, &db);
         adgMod::db->WaitForBackground();
         if (adgMod::MOD == 6 || adgMod::MOD == 7 || adgMod::MOD == 9) {
             Version* current = adgMod::db->versions_->current();

            // offline file learning
             current->FileLearn();
         }
         cout << "Shutting down" << endl;
        adgMod::db->WaitForBackground();
         delete db;*/
			}

			if (evict) rc = system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
	    (void) rc;

			size_t run_size = keys[wl].size();
			printf("%s: Starting up (wl%d, %d ops)\n", GetDayTime().c_str(), wl, num_operations);
			//std::vector<uint64_t> detailed_times;
	    bool start_new_event = true;
		  if (wl == 0) {
				status = DB::Open(options, db_location, &db);
				assert(status.ok() && "Open Error");
			}

			for (int i=0; i<run_size; i++) {
				if (start_new_event) {		// TODO
					start_new_event = false;
				}
				
				int op = ops[wl][i];
				if (op == OP_INSERT || op == OP_UPDATE) {
					status = db->Put(write_options, keys[wl][i], w_value);
					assert(status.ok() && "Key Put Error");
					//if (!status.ok()) fprintf(stderr, "Key %s Put Error\n", keys[wl][i].c_str());
				} else if (op == OP_READ) {
					string r_value;
					status = db->Get(read_options, keys[wl][i], &r_value);
					assert(status.ok() && "Key Not Found");
					//if (!status.ok()) fprintf(stderr, "Key %s Not Found\n", keys[wl][i].c_str());
				} else if (op == OP_SCAN) {
					fprintf(stderr, "TODO"); continue;
				} else { fprintf(stderr, "Unsupported operation\n"); exit(1); }

				// collect data every 1/10 of the run (history)
				if ((i+1) % (run_size/10) == 0) {		// TODO
					start_new_event = true;
			      /*cout << "Progress:" << (i + 1) / (num_operations / 10) * 10 << "%" << endl;
		        Version* current = koo::db->GetCurrentVersion();
	          printf("LevelSize %lu %lu %lu %lu %lu %lu\n", current->NumFiles(0), current->NumFiles(1), 
									current->NumFiles(2), current->NumFiles(3), current->NumFiles(4), current->NumFiles(5));
						koo::db->ReturnCurrentVersion(current);*/
				}
			}
			printf("%s: Finished (wl%d, %d ops)\n", GetDayTime().c_str(), wl, num_operations);
		}

    // report various data after the run
    delete db;
    printf("%s: Finish deleting db\n", GetDayTime().c_str());

    // print out averages
    // TODO
}
