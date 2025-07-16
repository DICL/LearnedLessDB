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
		/*seg_infos.clear();
		segs_inputs[0].clear();
		segs_inputs[1].clear();*/
		segs_output.clear();
	}

	/*struct SegInfo {
		SegInfo(uint64_t _x, uint64_t _x_last, uint32_t _num)
			: x(_x), x_last(_x_last), num_keys(_num) {}

		uint64_t x;
		uint64_t x_last;
		uint32_t num_keys;
	}*/

	Segment MakeSegment(uint64_t x, double y, uint64_t x_last, double y_last);
	bool Merge();
	void WriteModel();

	uint64_t num_entries;			// = LearnedIndexData->size
	uint64_t file_number;			// File number of the output file
	int level;								// Level of the output file

	uint64_t begin_key;
	uint64_t end_key;
	std::vector<std::pair<uint64_t, uint32_t>> seg_infos;		// <x_last, num_keys>
	//std::vector<SegInfo> seg_infos;
	
	std::vector<Segment> segs_output;
};


/*class MergeModel {
public:
	MergeModel()
		:	begin_seg1(0),
			end_seg1(0),
			begin_seg2(0),
			end_seg2(0),
			cur_x_last_idx_last(0),
			last_y_last1(0),
			last_y_last2(0),
			num_drops(0) {}

	MergeModel(const MergeModel& copy)
		:	begin_seg1(0),
			end_seg1(0),
			begin_seg2(0),
			end_seg2(0),
			cur_x_last_idx_last(0),
			last_y_last1(0),
			last_y_last2(0),
			num_drops(0) {}
	
	~MergeModel() {
		_seg_x_lasts.clear();
		_seg_num_drops.clear();
		segs.clear();
	}
	
	void AttachModels(std::vector<Segment>& _segs1,
										std::vector<Segment>& _segs2);

	void FindCuttingPoints(std::vector<Segment>& _segs1,
												std::vector<Segment>& _segs2);

	bool MakeInputs(std::vector<Segment>& _segs1,
									std::vector<Segment>& _segs2);

	// Part 1 & Part 2 & Part 3
	bool Merge();		// the first key(x) of the first seg

	// file_data에 merged model 넣기
	bool WriteModel();		// TODO bourbon 어떻게 모델 기록하는지 참고

	// 변하지 않는 값 (_x)
	std::vector<uint64_t> _seg_x_lasts;
	int _size;			// _seg_x_lasts.size()
	int _size1;			// _segs1.size()
	int _size2;			// _segs2.size()
	std::vector<uint32_t> _seg_num_drops;

	// output for문 돌때마다 변하는/새로 할당하는 값 (x)
	uint64_t begin;
	uint64_t end;
	uint64_t min_key1;
	uint64_t min_key2;
	int size;			// seg_x_lasts.size()
	int size1;		// segs1.size()
	int size2;		// segs2.size()
	//std::string filename;		// New SST name created after compaction
	uint64_t file_number;		// New SST number created after compaction
	std::vector<Segment> segs;
	uint32_t num_drops;		// Accumulated number of dropped keys

	std::vector<Segment> segs1;
	std::vector<Segment> segs2;
	std::vector<uint64_t> seg_x_lasts;
	std::vector<int> seg_num_drops;

private:
	int begin_seg1;
	int end_seg1;
	int begin_seg2;
	int end_seg2;
	int cur_x_last_idx_last;	// last cutting point(index) of seg_x_lasts
	uint32_t last_y_last1;		// for y_last of next output iteration
	uint32_t last_y_last2;
};*/

}
