#pragma once

#include <vector>
#include <utility>
#include <string>
#include "koo/plr.h"
#include "koo/util.h"
#include "koo/learned_index.h"
#include "koo/koo.h"


namespace koo {

// One MergeModel per one output file
class MergeModel {
public:
	MergeModel()
		: num_entries(0),
			file_number(0),
			level(0),
			begin_key(0),
			end_key(0) {}

	MergeModel(const MergeModel& copy)
		: num_entries(0),
			file_number(0),
			level(0),
			begin_key(0),
			end_key(0) {}

	~MergeModel() {
		segs_output.clear();
	}

	Segment MakeSegment(uint64_t x, double y, uint64_t x_last, double y_last);
	bool Merge();

	uint64_t num_entries;			// = LearnedIndexData->size
	uint64_t file_number;			// File number of the output file
	int level;								// Level of the output file

	uint64_t begin_key;
	uint64_t end_key;
	std::vector<std::pair<uint64_t, uint32_t>> seg_infos;		// <x_last, num_keys>
	
	std::vector<Segment> segs_output;
};


}
