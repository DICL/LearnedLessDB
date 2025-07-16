#include "koo/util.h"
#include "koo/learned_index.h"

namespace koo {


	FileLearnedIndexData* file_data = nullptr;
	leveldb::Env* env;
	leveldb::DBImpl* db;
	leveldb::ReadOptions read_options;
	leveldb::WriteOptions write_options;
	uint64_t initial_time = 0;

	//uint64_t fd_limit = 1024 * 1024;
	bool fresh_write = false;
	bool block_num_entries_recorded = false;
	uint64_t block_num_entries = 0;
	uint64_t block_size = 0;
	uint64_t entry_size = 0;
	float reference_frequency = 2.5;		// apache
	//float reference_frequency = 2.6;	// bourbon default

#if L0_MAP
	std::unordered_map<uint64_t, int> l0_map;
#endif
}
