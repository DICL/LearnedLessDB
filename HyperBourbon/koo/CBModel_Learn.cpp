#include <util/mutexlock.h>
#include "CBModel_Learn.h"
#include "learned_index.h"
#include "koo/koo.h"
#include "db/version_set.h"


CBModel_Learn::CBModel_Learn() : negative_lookups_time(2), positive_lookups_time(2) {};

void CBModel_Learn::AddLookupData(int level, bool positive, bool model, uint64_t value) {
#if !BOURBON_PLUS
    leveldb::MutexLock guard(&lookup_mutex);
#endif
    std::vector<Counter>& target = positive ? positive_lookups_time : negative_lookups_time;
    target[model].Increment(level, value);
}

void CBModel_Learn::AddFileData(int level, uint64_t num_negative, uint64_t num_positive, uint64_t size) {
#if !BOURBON_PLUS
    leveldb::MutexLock guard(&file_mutex);
#endif
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
#if !BOURBON_PLUS
        leveldb::MutexLock guard(&lookup_mutex);
#endif
        for (int i = 0; i < 2; ++i) {
#if BOURBON_PLUS
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
#else
            num_pos[i] = positive_lookups_time[i].nums[level];
            num_neg[i] = negative_lookups_time[i].nums[level];
            time_pos[i] = positive_lookups_time[i].counts[level];
            time_neg[i] = negative_lookups_time[i].counts[level];
#endif
            //std::cout << level << " " << num_pos[i] << " " << num_neg[i] << " " << time_pos[i] << " " << time_neg[i] << std::endl;	// KOO
        }
    }
    {
#if BOURBON_PLUS
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
#else
        leveldb::MutexLock guard(&file_mutex);
        num_files = num_negative_lookups_file.nums[level];
        num_neg_lookups_file = num_negative_lookups_file.counts[level];
        num_pos_lookups_file = num_positive_lookups_file.counts[level];
        size_sum = file_sizes.counts[level];
#endif
    }
#if 0 && BLEARN
		double tmp = MaxBytesForLevel(0);
		// scale 맞추기
		//std::cout << "\nlevel = " << level << "\tsize_sum = " << size_sum << "\tnum_files = " << num_files << std::endl;
		double d_size_sum = (double)size_sum;
		double d_num_files = (double)num_files;
		if (level == 0) { 
			d_size_sum *= (10*1048576.0/MaxBytesForLevel(0)); 
			d_num_files *= ((10*1048576.0/2*1048576.0)/(MaxBytesForLevel(0)/(double)MaxFileSizeForLevel(0)));
		} else if (level == 1) {
			d_size_sum *= (10*1048576.0/MaxBytesForLevel(1)); 
			d_num_files *= ((10*1048576.0/2*1048576.0)/(MaxBytesForLevel(1)/(double)MaxFileSizeForLevel(1)));
		} else if (level == 2) {
			d_size_sum *= (100*1048576.0/MaxBytesForLevel(2)); 
			d_num_files *= ((100*1048576.0/2*1048576.0)/(MaxBytesForLevel(2)/(double)MaxFileSizeForLevel(2)));
		} else if (level == 3) {
			d_size_sum *= (1000*1048576.0/MaxBytesForLevel(3)); 
			d_num_files *= ((1000*1048576.0/2*1048576.0)/(MaxBytesForLevel(3)/(double)MaxFileSizeForLevel(3)));
		} else if (level == 4) {
			d_size_sum *= (10000*1048576.0/MaxBytesForLevel(4)); 
			d_num_files *= ((10000*1048576.0/2*1048576.0)/(MaxBytesForLevel(4)/(double)MaxFileSizeForLevel(4)));
		} else if (level == 5) {
			d_size_sum *= (100000*1048576.0/MaxBytesForLevel(5)); 
			d_num_files *= ((100000*1048576.0/2*1048576.0)/(MaxBytesForLevel(5)/(double)MaxFileSizeForLevel(5)));
		} else if (level == 6) {
			d_size_sum *= (1000000*1048576.0/MaxBytesForLevel(6)); 
			d_num_files *= ((1000000*1048576.0/2*1048576.0)/(MaxBytesForLevel(6)/(double)MaxFileSizeForLevel(6)));
		} else std::cout << "Level error\n";

		/*else if (level == 1) { d_size_sum *= (10*1048576.0/MaxBytesForLevel(1)); d_num_files *= ((10.0/2.0)/(128.0/16.0)); }
		else if (level == 2) { d_size_sum *= (100*1048576.0/MaxBytesForLevel(2)); d_num_files *= ((100.0/2.0)/(512.0/16.0)); }
		else if (level == 3) { d_size_sum *= (1000*1048576.0/MaxBytesForLevel(3)); d_num_files *= ((1000.0/2.0)/(4096.0/16.0)); }
		else if (level == 4) { d_size_sum *= (10000*1048576.0/MaxBytesForLevel(4)); d_num_files *= ((10000.0/2.0)/(32768.0/16.0)); }
		else if (level == 5) { d_size_sum *= (100000*1048576.0/MaxBytesForLevel(5)); d_num_files *= ((100000.0/2.0)/(262144.0/16.0)); }
		else if (level == 6) { d_size_sum *= (1000000*1048576.0/MaxBytesForLevel(6)); d_num_files *= ((1000000.0/2.0)/(2097152.0/16.0)); }*/
		size_sum = (uint64_t)(std::round(d_size_sum));
		num_files = (int)(std::round(d_num_files));
		//std::cout << "d_size_sum = " << std::to_string(d_size_sum) << "\tsize_sum = " << size_sum << std::endl;
		//std::cout << "d_num_files = " << std::to_string(d_num_files) << "\tnum_files = " << num_files << std::endl;
#endif
//    num_learn = learn_costs.nums[level];
//    cost_sum = learn_costs.counts[level];
//    learn_size_sum = learn_sizes.counts[level];

		//std::cout << "level = " << level << " -> " << num_files << " " << file_average_limit[level] << std::endl;
    if (num_files < file_average_limit[level]) return const_size_to_cost + 1;
    double average_pos_lookups = (double) num_pos_lookups_file / num_files;
    double average_neg_lookups = (double) num_neg_lookups_file / num_files;
    double average_pos_time[2] = {0, 0}, average_neg_time[2] = {0, 0};

    //for (int i = 0; i < 2; ++i) {
    for (int i = 1; i > -1; --i) {		// model부터
        if (num_pos[i] + num_neg[i] < lookup_average_limit) {		// index block, model search 둘 다 lookup_average_limit 이상이어야 학습
					//std::cout << i << " " << num_pos[i] << " " << num_neg[i] << " " << num_pos[!i] << " " << num_neg[!i] << std::endl;
					if (i) continue;	// model search 0일때는 continue해주기 
					//continue;
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

//    model->num_pos_baseline_p = num_pos[0];
//    model->num_pos_model_p = num_pos[1];
//    model->num_neg_baseline_p = num_neg[0];
//    model->num_neg_model_p = num_neg[1];
//    model->time_pos_baseline_p = time_pos[0];
//    model->time_pos_model_p = time_pos[1];
//    model->time_neg_baseline_p = time_neg[0];
//    model->time_neg_model_p = time_neg[1];
//    model->file_size = file_size;
//    model->gain_p = pos_gain + neg_gain;
//    //std::cout << model->gain_p << std::endl;
//    model->num_files_p = num_files;

    // used for simple testing different learning policies, not used now
    if (koo::policy == 1) return const_size_to_cost + 1;
    if (koo::policy == 2) return 0;

    //fprintf(stdout, "%f %lu %d %f %f %f %f %f %f\n", pos_gain + neg_gain, file_size * const_size_to_cost, level,
    //         average_pos_time[0], average_pos_time[1], average_pos_lookups, average_neg_time[0], average_neg_time[1], average_neg_lookups);
		static int total = 0, exceed = 0;
		/*std::ofstream ofs("/koo/HyperBourbon/koo/data/scores.txt", std::ios::app);
		ofs << ++total << ": " << std::to_string(pos_gain) << " " << std::to_string(neg_gain) << " " << size_sum << " " << num_files << " " << file_size;
		double score = (pos_gain + neg_gain) / size_sum * num_files;
		if (score > const_size_to_cost) ofs << "\t\tlevel: " << level << ", score: " << std::to_string(score) << ", " << ++exceed << std::endl;
		else ofs << "\t\tlevel: " << level << ", score: " << std::to_string(score) << std::endl;
		ofs.close();*/
    return (pos_gain + neg_gain) / size_sum * num_files;//(num_learn >= 20 ? (double) cost_sum / learn_size_sum : const_size_to_cost);
}



void CBModel_Learn::Report() {
#if !BOURBON_PLUS
    negative_lookups_time[0].name = "BaselineNegative";
    negative_lookups_time[1].name = "LLSMNegative";
    positive_lookups_time[0].name = "BaselinePositive";
    positive_lookups_time[1].name = "LLSMPositive";

    negative_lookups_time[0].Report();
    negative_lookups_time[1].Report();
    positive_lookups_time[0].Report();
    positive_lookups_time[1].Report();
#endif
}
























