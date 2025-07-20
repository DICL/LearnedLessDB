#include "generator.h"

#include <atomic>
#include <mutex>
#include <random>
#include <vector>


namespace ycsbc {

class SOSDGenerator : public Generator<uint64_t> {
 public:
  // Both min and max are inclusive
  SOSDGenerator() { 
  	counter_.store(0);

		std::string filename = koo::sosd_data_path;
  	std::ifstream in(filename, std::ios::binary);
  	if (!in.is_open()) {
  		fprintf(stderr, "unable to open %s\n", filename.c_str());
  		exit(1);
		}
		uint64_t size;
		in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
		keys.resize(size);
		in.read(reinterpret_cast<char*>(keys.data()), size * sizeof(uint64_t));
		in.close();

		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		std::mt19937 generator(seed);
		std::shuffle(keys.begin(), keys.end(), generator);

		fprintf(stdout, "Read finished\n");
		fflush(stdout);

  	Next(); 
  }
  
  uint64_t Next() { return keys[counter_.fetch_add(1)]; }
  uint64_t Last() { return keys[counter_.load() - 1]; }
  void Set(uint64_t start) { counter_.store(start); }
  
 private:
	std::vector<uint64_t> keys;
	std::atomic<uint64_t> counter_;
};

} // ycsbc

