#include <iostream>
#include <vector>
#include <math.h>
#include <time.h>
#include <string>
#include <utility>
#include <fstream>
#include <algorithm>
#include "koo/merge.h"


namespace koo {

Segment MergeModel::MakeSegment(uint64_t x, double y,
																uint64_t x_last, double y_last) {
	uint64_t x_delta = x_last - x;
	double y_delta = y_last - y;
	if (y_delta <= 0) { 
		y_delta = 1; 
	}
	if (x_delta+1 < y_delta) y_delta = x_delta + 1;

	double a = y_delta / static_cast<double>(x_delta);
#if NORMARLIZE_KEY
	uint64_t nor_key = x_last - begin_key + 1;
	double b = std::floor(y_last) - a * static_cast<double>(nor_key);
#else
	double b = std::floor(y_last) - a * x_last;
#endif
	return Segment(x, a, b, x_last, std::floor(y_last));
}

bool MergeModel::Merge() {
	uint64_t x = begin_key;
	uint32_t y = 0, y_last = 0;
	int seg_infos_size = seg_infos.size();

	bool pass = false;
	for (int i=0; i<seg_infos_size-1; i++) {
		uint64_t x_last = seg_infos[i].first;
		uint32_t num_keys = seg_infos[i].second;
		if (end_key < x_last) {
			segs_output.push_back(MakeSegment(x, y, end_key, num_entries-1));
			pass = true;
			break;
		}

		y_last += num_keys;
		if (y_last - y < koo::min_num_keys) continue;

		segs_output.push_back(MakeSegment(x, y, x_last, y_last));
		x = x_last + 1;
		y = y_last + 1;
	}
	if (!pass) segs_output.push_back(MakeSegment(x, y, end_key, num_entries-1));

	// Attah last segment
	if (!segs_output.size()) {
		return false;
	}
	if (!(segs_output.back().k == 0 && segs_output.back().b == 0))
		segs_output.emplace_back(Segment(end_key, 0, 0, 0, 0));

	return true;
}

}
