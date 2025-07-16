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
	//if (y >= y_last) assert("MergeModel::MakeSegment y >= y_last");
	uint64_t x_delta = x_last - x;
	double y_delta = y_last - y;
	if (y_delta <= 0) { 
		//std::cout << __LINE__ << ": y_delta <= 0   y_delta = " << y_delta << ", y = " << y << ", y_last = " << y_last; 
		//std::cout << ", file_numer = " << file_number << std::endl;
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
		/*if (i) x = seg_infos[i].x;
		uint64_t x_last = seg_infos[i].x_last;
		uint32_t num_keys = seg_infos[i].num_keys;*/
		uint64_t x_last = seg_infos[i].first;
		uint32_t num_keys = seg_infos[i].second;
		if (end_key < x_last) {
			segs_output.push_back(MakeSegment(x, y, end_key, num_entries-1));
			pass = true;
			break;
		}

		y_last += num_keys;
		//if (num_keys < SKIP_SEG) continue;		// default
		if (y_last - y < SKIP_SEG) continue;

		segs_output.push_back(MakeSegment(x, y, x_last, y_last));
		x = x_last + 1;
		y = y_last + 1;
	}
	if (!pass) segs_output.push_back(MakeSegment(x, y, end_key, num_entries-1));

	// Attah last segment
	if (!segs_output.size()) {
		std::cout << "!!!!!!!!!!!!!!!ERROR 11 " << file_number << std::endl;
		return false;
	}
	if (!(segs_output.back().k == 0 && segs_output.back().b == 0))
		segs_output.emplace_back(Segment(end_key, 0, 0, 0, 0));

#if DEBUG
	WriteModel();
#endif

	return true;
}

void MergeModel::WriteModel() {
	std::string number = std::to_string(file_number);
/*	std::ofstream of_info("/koo/HyperLearningless/koo/data/segs_" + number + "_infos.txt");
	for (auto& info : seg_infos)
		of_info << info.first << " " << info.second << std::endl;
	of_info.close();*/
	std::ofstream of("/koo/HyperLearningless/koo/data/segs_" + number + ".txt");
	of.precision(15);
	for (auto& s : segs_output) {
		of << "(" << s.x << ", )~(" << s.x_last << ", " << s.y_last << "): y = " << s.k << " * x + " << s.b << "\n";
		if (s.x >= s.x_last && s.x != end_key)
			std::cout << "ERROR 8: " << file_number << std::endl;
	}
	of.close();
}

}
