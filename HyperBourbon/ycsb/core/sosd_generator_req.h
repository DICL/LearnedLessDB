#include "generator.h"

#include <atomic>
#include <mutex>
#include <random>
#include <vector>
#include "koo/koo.h"


#if YCSB_SOSD
namespace ycsbc {

class SOSDReqGenerator : public Generator<uint64_t> {
 public:
  // Both min and max are inclusive
  SOSDReqGenerator(uint32_t which) { 
  	counter_.store(0);

		std::string filename = "";
		if (which == 0) filename = "books_600M";
		else if (which == 1) filename = "osm_cellids_600M";
		else if (which == 2) filename = "normal_600M";
		else if (which == 3) filename = "lognormal_600M";
		else if (which == 4) filename = "uniform_dense_600M";
		else if (which == 5) filename = "uniform_sparse_600M";
		else if (which == 6) filename = "fb_200M";
		else if (which == 7) filename = "wiki_ts_200M";

  	std::ifstream in("/koo/SOSD/data/lookups/"+filename+"_uint64_equality_lookups_10M", std::ios::binary);
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

		fprintf(stdout, "Read finished req\n");
		fflush(stdout);

  	Next(); 
  }
  
  uint64_t Next() { return lookups[counter_.fetch_add(1)]; }
  uint64_t Last() { return lookups[counter_.load() - 1]; }
  
 private:
	std::vector<uint64_t> lookups;
	std::atomic<uint64_t> counter_;
};

} // ycsbc
#endif

