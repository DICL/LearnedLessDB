#include "generator.h"

#include <atomic>
#include <mutex>
#include <random>
#include <vector>


namespace ycsbc {

class SOSDReqGenerator : public Generator<uint64_t> {
 public:
  // Both min and max are inclusive
  SOSDReqGenerator() { 
  	counter_.store(0);

		std::string filename = koo::sosd_lookups_path;
  	std::ifstream in(filename, std::ios::binary);
  	if (!in.is_open()) {
  		fprintf(stderr, "unable to open %s\n", filename.c_str());
  		exit(1);
		}
		uint64_t size;
		in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
		lookups.resize(size);
		in.read(reinterpret_cast<char*>(lookups.data()), size * sizeof(uint64_t));
		in.close();

		unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
		std::mt19937 generator(seed);
		std::shuffle(lookups.begin(), lookups.end(), generator);

  	Next(); 
  }
  
  uint64_t Next() { return lookups[counter_.fetch_add(1)]; }
  uint64_t Last() { return lookups[counter_.load() - 1]; }
  
 private:
	std::vector<uint64_t> lookups;
	std::atomic<uint64_t> counter_;
};

} // ycsbc

