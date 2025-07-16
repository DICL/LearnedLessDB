#ifndef LEVELDB_UTIL_H
#define LEVELDB_UTIL_H

#include "db/db_impl.h"
#include "hyperleveldb/slice.h"
#include "hyperleveldb/env.h"
#include <x86intrin.h>
#include "koo/koo.h"

using leveldb::Slice;

namespace koo {

	class FileLearnedIndexData;
	class LearnedIndexData;


	extern FileLearnedIndexData* file_data;
	extern leveldb::Env* env;
	extern leveldb::DBImpl* db;
	extern leveldb::ReadOptions read_options;
	extern leveldb::WriteOptions write_options;
	extern uint64_t initial_time;

	//extern uint64_t fd_limit;
	extern bool fresh_write;
	extern bool block_num_entries_recorded;
	extern uint64_t block_num_entries;
	extern uint64_t block_size;
	extern uint64_t entry_size;
	extern float reference_frequency;

#if L0_MAP
	extern std::unordered_map<uint64_t, int> l0_map;
#endif
}

#endif	// LEVELDB_UTIL_H
