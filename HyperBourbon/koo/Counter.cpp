//
// Created by daiyi on 2020/02/12.
//

#include <iostream>
#include <numeric>
#include "Counter.h"


void Counter::Increment(int level, uint64_t n) {
#if BOURBON_PLUS
	if (level == 0) { count0 += n; num0 += 1; }
	else if (level == 1) { count1 += n; num1 += 1; }
	else if (level == 2) { count2 += n; num2 += 1; }
	else if (level == 3) { count3 += n; num3 += 1; }
	else if (level == 4) { count4 += n; num4 += 1; }
	else if (level == 5) { count5 += n; num5 += 1; }
	else if (level == 6) { count6 += n; num6 += 1; }
#else
    counts[level] += n;
    nums[level] += 1;
#endif
}

void Counter::Reset() {
#if BOURBON_PLUS
	count0.store(0);
	count1.store(0);
	count2.store(0);
	count3.store(0);
	count4.store(0);
	count5.store(0);
	count6.store(0);
	num0.store(0);
	num1.store(0);
	num2.store(0);
	num3.store(0);
	num4.store(0);
	num5.store(0);
	num6.store(0);
#else
    for (uint64_t& count : counts) count = 0;
    for (uint64_t& num : nums) num = 0;
#endif
}

#if !BOURBON_PLUS
void Counter::Report() {
    std::cout << "Counter " << name << " " << Sum();
    for (uint64_t count : counts) {
        std::cout << " " << count;
    }
    std::cout << "\n";
    std::cout << NumSum();
    for (uint64_t num : nums) {
        std::cout << " " << num;
    }
    std::cout << "\n";
}

int Counter::Sum() {
    return std::accumulate(counts.begin(), counts.end(), 0.0);
}

int Counter::NumSum() {
    return std::accumulate(nums.begin(), nums.end(), 0.0);
}
#endif
