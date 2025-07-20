#include <util/mutexlock.h>
#include "CBModel_Learn.h"
#include "learned_index.h"
#include "koo/koo.h"
#include "db/version_set.h"


CBModel_Learn::CBModel_Learn() : negative_lookups_time(2), positive_lookups_time(2) {};

void CBModel_Learn::AddLookupData(int level, bool positive, bool model, uint64_t value) {
    std::vector<Counter>& target = positive ? positive_lookups_time : negative_lookups_time;
    target[model].Increment(level, value);
}

void CBModel_Learn::AddFileData(int level, uint64_t num_negative, uint64_t num_positive, uint64_t size) {
    num_negative_lookups_file.Increment(level, num_negative);
    num_positive_lookups_file.Increment(level, num_positive);
    file_sizes.Increment(level, size);
}

void CBModel_Learn::AddLearnCost(int level, uint64_t cost, uint64_t size) {
    learn_costs.Increment(level, cost);
    learn_sizes.Increment(level, size);
}

double CBModel_Learn::CalculateCB(int level, uint64_t file_size) {
    // used for simple testing different learning policies, not used now
    if (koo::policy == 2) return 0;

    int num_pos[2] = {0, 0}, num_neg[2] = {0, 0}, num_files = 0, num_learn = 0;
    uint64_t time_pos[2] = {0, 0}, time_neg[2] = {0, 0}, num_neg_lookups_file, num_pos_lookups_file, size_sum, cost_sum, learn_size_sum;
    {
        for (int i = 0; i < 2; ++i) {
        	if (level == 0) {
            num_pos[i] = positive_lookups_time[i].num0;
            num_neg[i] = negative_lookups_time[i].num0;
            time_pos[i] = positive_lookups_time[i].count0;
            time_neg[i] = negative_lookups_time[i].count0;
					} else if (level == 1) {
            num_pos[i] = positive_lookups_time[i].num1;
            num_neg[i] = negative_lookups_time[i].num1;
            time_pos[i] = positive_lookups_time[i].count1;
            time_neg[i] = negative_lookups_time[i].count1;
					} else if (level == 2) {
            num_pos[i] = positive_lookups_time[i].num2;
            num_neg[i] = negative_lookups_time[i].num2;
            time_pos[i] = positive_lookups_time[i].count2;
            time_neg[i] = negative_lookups_time[i].count2;
					} else if (level == 3) {
            num_pos[i] = positive_lookups_time[i].num3;
            num_neg[i] = negative_lookups_time[i].num3;
            time_pos[i] = positive_lookups_time[i].count3;
            time_neg[i] = negative_lookups_time[i].count3;
					} else if (level == 4) {
            num_pos[i] = positive_lookups_time[i].num4;
            num_neg[i] = negative_lookups_time[i].num4;
            time_pos[i] = positive_lookups_time[i].count4;
            time_neg[i] = negative_lookups_time[i].count4;
					} else if (level == 5) {
            num_pos[i] = positive_lookups_time[i].num5;
            num_neg[i] = negative_lookups_time[i].num5;
            time_pos[i] = positive_lookups_time[i].count5;
            time_neg[i] = negative_lookups_time[i].count5;
					} else if (level == 6) {
            num_pos[i] = positive_lookups_time[i].num6;
            num_neg[i] = negative_lookups_time[i].num6;
            time_pos[i] = positive_lookups_time[i].count6;
            time_neg[i] = negative_lookups_time[i].count6;
					}
        }
    }
    {
			if (level == 0) {
        num_files = num_negative_lookups_file.num0;
        num_neg_lookups_file = num_negative_lookups_file.count0;
        num_pos_lookups_file = num_positive_lookups_file.count0;
        size_sum = file_sizes.count0;
			} else if (level == 1) {
        num_files = num_negative_lookups_file.num1;
        num_neg_lookups_file = num_negative_lookups_file.count1;
        num_pos_lookups_file = num_positive_lookups_file.count1;
        size_sum = file_sizes.count1;
			} else if (level == 2) {
        num_files = num_negative_lookups_file.num2;
        num_neg_lookups_file = num_negative_lookups_file.count2;
        num_pos_lookups_file = num_positive_lookups_file.count2;
        size_sum = file_sizes.count2;
			} else if (level == 3) {
        num_files = num_negative_lookups_file.num3;
        num_neg_lookups_file = num_negative_lookups_file.count3;
        num_pos_lookups_file = num_positive_lookups_file.count3;
        size_sum = file_sizes.count3;
			} else if (level == 4) {
        num_files = num_negative_lookups_file.num4;
        num_neg_lookups_file = num_negative_lookups_file.count4;
        num_pos_lookups_file = num_positive_lookups_file.count4;
        size_sum = file_sizes.count4;
			} else if (level == 5) {
        num_files = num_negative_lookups_file.num5;
        num_neg_lookups_file = num_negative_lookups_file.count5;
        num_pos_lookups_file = num_positive_lookups_file.count5;
        size_sum = file_sizes.count5;
			} else if (level == 6) {
        num_files = num_negative_lookups_file.num6;
        num_neg_lookups_file = num_negative_lookups_file.count6;
        num_pos_lookups_file = num_positive_lookups_file.count6;
        size_sum = file_sizes.count6;
			}
    }
//    num_learn = learn_costs.nums[level];
//    cost_sum = learn_costs.counts[level];
//    learn_size_sum = learn_sizes.counts[level];

    if (num_files < file_average_limit[level]) return const_size_to_cost + 1;
    double average_pos_lookups = (double) num_pos_lookups_file / num_files;
    double average_neg_lookups = (double) num_neg_lookups_file / num_files;
    double average_pos_time[2] = {0, 0}, average_neg_time[2] = {0, 0};

    for (int i = 1; i > -1; --i) {
        if (num_pos[i] + num_neg[i] < lookup_average_limit) {
					if (i) continue;	// model search 0일때는 continue해주기 
          return 0;
        }

        if (num_pos[i] < 500) {
            average_pos_lookups = 0;
            average_pos_time[i] = 0;
        } else {
            average_pos_time[i] = (double) time_pos[i] / num_pos[i];
        }

        if (num_neg[i] < 500) {
            average_neg_lookups = 0;
            average_neg_time[i] = 0;
        } else {
            average_neg_time[i] = (double) time_neg[i] / num_neg[i];
        }
    }

    double pos_gain = (average_pos_time[0] - average_pos_time[1]) * average_pos_lookups;
    double neg_gain = (average_neg_time[0] - average_neg_time[1]) * average_neg_lookups;
    // 0: non-model search Get time, 1: model search Get time

    // used for simple testing different learning policies, not used now
    if (koo::policy == 1) return const_size_to_cost + 1;
    if (koo::policy == 2) return 0;

    //         average_pos_time[0], average_pos_time[1], average_pos_lookups, average_neg_time[0], average_neg_time[1], average_neg_lookups);
		static int total = 0, exceed = 0;
    return (pos_gain + neg_gain) / size_sum * num_files;//(num_learn >= 20 ? (double) cost_sum / learn_size_sum : const_size_to_cost);
}



void CBModel_Learn::Report() {
}
























