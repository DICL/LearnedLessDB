
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
uint64_t LOAD_SIZE;
size_t MAX_NUM_WORKLOAD = 6;

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

enum LoadType {
    Ordered = 0,
    Reversed = 1,
    ReversedChunk = 2,
    Random_ = 3,
    RandomChunk = 4
};

int main(int argc, char *argv[]) {
    int rc;
    int num_operations, num_mix;
    string db_location, input_filename, distribution_filename;
    char* ycsb_filename;
    //bool print_single_timing, print_file_info;
    bool evict, use_distribution = false, pause, use_ycsb = false, fresh_write, only_load;
    int load_type, insert_bound;
    int value_size, num_threads;

    cxxopts::Options commandline_options("leveldb read test", "Testing leveldb read performance.");
    commandline_options.add_options()
            ("n,get_number", "the number of gets", cxxopts::value<int>(num_operations)->default_value("10000000"))
            ("d,directory", "the directory of db", cxxopts::value<string>(db_location)->default_value("/mnt-koo/db/"))
            ("k,key_size", "the size of key", cxxopts::value<int>(key_size)->default_value("16"))
            ("v,value_size", "the size of value", cxxopts::value<int>(value_size)->default_value("8"))
            //("single_timing", "print the time of every single get", cxxopts::value<bool>(print_single_timing)->default_value("false"))
            //("file_info", "print the file structure info", cxxopts::value<bool>(print_file_info)->default_value("false"))
            //("file_model_error", "error in file model", cxxopts::value<uint32_t>(adgMod::file_model_error)->default_value("8"))
            ("f,input_file", "the filename of input file", cxxopts::value<string>(input_filename)->default_value(""))
            ("YCSB", "use YCSB trace", cxxopts::value<char*>(ycsb_filename)->default_value(nullptr))
            ("w,write", "writedb", cxxopts::value<bool>(fresh_write)->default_value("true"))
            ("load,only_load", "exit after load", cxxopts::value<bool>(only_load)->default_value("false"))
            ("c,uncache", "evict cache", cxxopts::value<bool>(evict)->default_value("false"))
            ("l,load_type", "load type", cxxopts::value<int>(load_type)->default_value("0"))
            //("filter", "use filter", cxxopts::value<bool>(adgMod::use_filter)->default_value("false"))
            ("mix", "portion of writes in workload in 1000 operations", cxxopts::value<int>(num_mix)->default_value("0"))
            ("distribution", "operation distribution", cxxopts::value<string>(distribution_filename)->default_value(""))
            ("insert", "insert new value", cxxopts::value<int>(insert_bound)->default_value("0"))
            ("num_load", "the number of kvs to load", cxxopts::value<uint64_t>(LOAD_SIZE)->default_value("10000000"))
            ("threads", "the number of clients", cxxopts::value<int>(num_threads)->default_value("1"));
    auto result = commandline_options.parse(argc, argv);

    std::default_random_engine e1(0), e2(255), e3(0);
    srand(0);

    vector<string> keys[MAX_NUM_WORKLOAD];
		vector<uint64_t> distribution[MAX_NUM_WORKLOAD];
		vector<int> ycsb_is_write[MAX_NUM_WORKLOAD];
    if (!input_filename.empty()) {		// AR, OSM 쓸때 + SpanDB YCSB key
        ifstream input(input_filename);
        string key;
        while (input >> key) {
            string the_key = generate_key(key);
            keys.push_back(std::move(the_key));
        }
        LOAD_SIZE = keys.size();
    } else {					// 그 외 랜덤 키
				if (!ycsb_filename) fprintf(stderr, "If no input_filename, need ycsb_filename\n");
				keys.reserve(LOAD_SIZE);
        std::uniform_int_distribution<uint64_t> udist_key(0, 999999999999999);
        for (int i = 0; i < LOAD_SIZE; ++i) {
            keys.push_back(generate_key(to_string(udist_key(e2))));
        }
    }

    if (!distribution_filename.empty()) {
        use_distribution = true;
        ifstream input(distribution_filename);
        uint64_t index;
        while (input >> index) {
            distribution.push_back(index);
        }
    }

		// 코드 읽어보니까 distribution이 input_filename 있을땐 index고 없을땐 key값인듯?
		int num_ycsb = 0;
    if (ycsb_filename) {
        use_ycsb = true;
        use_distribution = true;
        while (ycsb_filename != nullptr) {
						char* sep = strchr(ycsb_filename, ',');
						string filename;
						if (sep == nullptr) {
							filename = ycsb_filename;
							ycsb_filename = nullptr;
						} else {
							filename = string(ycsb_filename, sep - ycsb_filename);
							ycsb_filename = sep + 1;
						}

				    ifstream input(filename);
		        uint64_t index;
						int is_write;
				    while (input >> is_write >> index) {
		            distributions[num_ycsb].push_back(index);
								ycsb_is_writes[num_ycsb].push_back(is_write);
						}
						num_ycsb++;
				}
    }

    if (num_mix > 1000) {
        mix_base = 1000;
        num_mix -= 1000;
    }

		// Start
    string values(1024 * 1024, '0');
    std::uniform_int_distribution<uint64_t> uniform_dist_file(0, (uint64_t)LOAD_SIZE - 1);
    std::uniform_int_distribution<uint64_t> uniform_dist_file2(0, (uint64_t)LOAD_SIZE - 1);
    //std::uniform_int_distribution<uint64_t > uniform_dist_value(0, (uint64_t) values.size() - value_size - 1);
    static char w_value_buf[4096];
    for (int i=0; i<4096; i++) w_value_buf[i] = (char)i;
    Slice w_value(w_value_buf, value_size);

    DB* db;
    Options options;
    ReadOptions read_options = ReadOptions();
    WriteOptions write_options = WriteOptions();
    Status status;
    options.create_if_missing = true;
    write_options.sync = false;

    if (fresh_write) {
				printf("%s: Load DB (%lu kvs)\n", GetDayTime().c_str(), LOAD_SIZE);
        // Load DB
        // clear existing directory, clear page cache, trim SSD
        string command = "rm -rf " + db_location;
        rc = system(command.c_str());
        status = DB::Open(options, db_location, &db);
        assert(status.ok() && "Open Error");

        // different load order
        int cut_size = LOAD_SIZE / 100000;
        std::vector<std::pair<int, int>> chunks;
        switch (load_type) {
            case Ordered: {
                for (int cut = 0; cut < cut_size; ++cut) {
										chunks.emplace_back(LOAD_SIZE * cut / cut_size, LOAD_SIZE * (cut + 1) / cut_size);
                }
								break;
            }
            case ReversedChunk: {
                for (int cut = cut_size - 1; cut >= 0; --cut) {
                    chunks.emplace_back(LOAD_SIZE * cut / cut_size, LOAD_SIZE * (cut + 1) / cut_size);
                }
                break;
            }
            case Random_: {
                std::random_shuffle(keys.begin(), keys.end());
                for (int cut = 0; cut < cut_size; ++cut) {
                    chunks.emplace_back(LOAD_SIZE * cut / cut_size, LOAD_SIZE * (cut + 1) / cut_size);
                }
                break;
            }
            case RandomChunk: {
                for (int cut = 0; cut < cut_size; ++cut) {
										chunks.emplace_back(LOAD_SIZE * cut / cut_size, LOAD_SIZE * (cut + 1) / cut_size);
                }
                std::random_shuffle(chunks.begin(), chunks.end());
								break;
            }
            default: assert(false && "Unsupported load type.");
        }

        // perform load
        for (int cut = 0; cut < chunks.size(); ++cut) {
						for (int i = chunks[cut].first; i < chunks[cut].second; ++i) {
                //status = db->Put(write_options, keys[i], {values.data() + uniform_dist_value(e2), (uint64_t) value_size});
                status = db->Put(write_options, keys[i], w_value);
								assert(status.ok() && "File Put Error");
            }
        }
				printf("%s: Load complete\n", GetDayTime().c_str());
				if (only_load) {
					delete db;
					printf("%s: Finish deleting db\n", GetDayTime().c_str());
				} else {
					koo::db->vlog->Sync();
				}

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

         /*keys.clear();			// TODO key를 다시 읽어야?
         if (!input_filename.empty()) {
             ifstream input(input_filename);
             string key;
             while (input >> key) {
                 string the_key = generate_key(key);
                 keys.push_back(std::move(the_key));
             }
				} else {					// 그 외 랜덤 키
						std::uniform_int_distribution<uint64_t> udist_key(0, 999999999999999);
		        for (int i = 0; i < 1000000; ++i) {
				        keys.push_back(generate_key(to_string(udist_key(e2))));
						}
		    }*/
    }

    if (evict) rc = system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
    (void) rc;

		printf("%s: Starting up (%d)\n", GetDayTime().c_str(), num_operations);
    uint64_t last_read = 0, last_write = 0;
    int last_level = 0, last_file = 0, last_baseline = 0, last_succeeded = 0, last_false = 0, last_compaction = 0, last_learn = 0;
    //std::vector<uint64_t> detailed_times;
    bool start_new_event = true;
    if (!fresh_write) {
			status = DB::Open(options, db_location, &db);
		  assert(status.ok() && "Open Error");
		}

    // perform workloads according to given distribution, read-write percentage, YCSB workload. (If they are set.)
    uint64_t write_i = 0;
    if (use_ycsb) {
    	assert(num_ycsb);
    	for (int wl=0; wl<num_ycsb; ++wl) {
    		for (int i=0; i<num_operations; ++i) {
	    		if (start_new_event) {	// TODO
		  			start_new_event = false;
					}

					bool write = ycsb_is_writes[wl][i] > 0;
					if (write) {
						if (input_filename.empty()) {			// used for ycsb default
							status = db->Put(write_options, generate_key(to_string(distributions[wl][i])), w_value);
						} else {
							uint64_t index = distributions[wl][i];
							
							if (ycsb_is_writes[wl][i] == 2) {		// ycsb insert TODO 그냥 이렇게 키 만들어도 되는거?
								status = db->Put(write_options, generate_key(to_string(10000000000+index)), w_value);
							} else {		// other write
								status = db->Put(write_options, keys[index], w_value);
							}
							assert(status.ok() && "Mix Put Error");
						}
					} else {		// read
						string value;
						if (input_filename.empty()) {			// ycsb default
							status = db->Get(read_options, generate_key(to_string(distributions[wl][i])), &value);
							if (!status.ok()) cout << distributions[wl][i] << " Not Found\n";
						} else {
							uint64_t index = distributions[wl][i];
							const string& key = keys[index];
							if (insert_bound != 0 && index > insert_bound) {	// read inserted key
								status = db->Get(read_options, generate_key(to_string(10000000000+index)), &value);
							} else {		// other
								status = db->Get(read_options, key, &value);
							}
							if (!status.ok()) cout << key << " Not Found\n";
						}
					}

					// collect data every 1/10 of the run TODO
				}
			}
		} else {
	    for (int i = 0; i < num_operations; ++i) {
        if (start_new_event) {
					//detailed_times.push_back(instance->GetTime());		TODO
          start_new_event = false;
        }

        bool write = (i % mix_base) < num_mix;
        if (write) {
					if (input_filename.empty()) {			// used for ycsb default
						status = db->Put(write_options, generate_key(to_string(distribution[i])), w_value);
          } else {
						uint64_t index;
            if (use_distribution) {
							index = distribution[i];
            } else if (load_type == 0) {		// Ordered
              index = write_i++ % LOAD_SIZE;
            } else {
              index = uniform_dist_file(e1) % (LOAD_SIZE - 1);
            }

						status = db->Put(write_options, keys[index], w_value);
						assert(status.ok() && "Mix Put Error");
					}
				} else {		// read
					string value;
          if (input_filename.empty()) {		// ycsb default
            status = db->Get(read_options, generate_key(to_string(distribution[i])), &value);
			      if (!status.ok()) cout << distribution[i] << " Not Found" << endl;
				  } else {
            uint64_t index = use_distribution ? distribution[i] : uniform_dist_file2(e2) % (LOAD_SIZE - 1);
            const string& key = keys[index];
            if (insert_bound != 0 && index > insert_bound) {		// read inserted key
               status = db->Get(read_options, generate_key(to_string(10000000000 + index)), &value);
            } else {			// other
								status = db->Get(read_options, key, &value);
            }

            if (!status.ok()) cout << key << " Not Found" << endl;
					}
        }

				// collect data every 1/10 of the run
        //if ((i + 1) % (num_operations / 100) == 0) detailed_times.push_back(instance->GetTime());	TODO
        if ((i + 1) % (num_operations / 10) == 0) {
          start_new_event = true;
          cout << "Progress:" << (i + 1) / (num_operations / 10) * 10 << "%" << endl;
          Version* current = koo::db->GetCurrentVersion();
          printf("LevelSize %lu %lu %lu %lu %lu %lu\n", current->NumFiles(0), current->NumFiles(1), 
									current->NumFiles(2), current->NumFiles(3), current->NumFiles(4), current->NumFiles(5));
					koo::db->ReturnCurrentVersion(current);
        }
			}
		}
    printf("%s: Finished\n", GetDayTime().c_str());

    // report various data after the run
    delete db;
    printf("%s: Finish deleting db\n", GetDayTime().c_str());

    // print out averages
    // TODO
}
