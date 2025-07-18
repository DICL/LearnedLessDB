#include "koo/koo.h"
#include "koo/util.h"

using std::to_string;

namespace koo {
std::string model_dbname = "/models";
double learn_model_error = 8;
double merge_model_error = 21;

#if LOOKUP_ACCURACY
std::atomic<uint64_t> lm_error[7];
std::atomic<uint64_t> lm_num_error[7];
std::atomic<uint64_t> mm_error[7];
std::atomic<uint64_t> mm_num_error[7];
std::atomic<uint64_t> rm_error[7];
std::atomic<uint64_t> rm_num_error[7];
#endif

#if MODEL_ACCURACY
std::atomic<uint64_t> lm_max_error[7];
std::atomic<uint64_t> lm_avg_error[7];
std::atomic<uint64_t> lm_num_error2[7];
std::atomic<uint64_t> mm_max_error[7];
std::atomic<uint64_t> mm_avg_error[7];
std::atomic<uint64_t> mm_num_error2[7];

std::atomic<uint64_t> mm_max_error_over[7];
std::atomic<uint64_t> mm_avg_error_over[7];
std::atomic<uint64_t> mm_num_error_over[7];
std::atomic<uint64_t> mm_cnt_overmax[7];
#endif

#if MODEL_BREAKDOWN
std::atomic<uint64_t> lm_segs[7];	// learned model # of segs
std::atomic<uint64_t> lm_keys[7];	// learned model # of segs
std::atomic<uint64_t> lm_num[7];
std::atomic<uint64_t> mm_segs[7];	// merged model
std::atomic<uint64_t> mm_keys[7];	// merged model
std::atomic<uint64_t> mm_num[7];

uint64_t num_lm[7];		// # of learned models
uint64_t num_mm[7];		// # of merged models
#endif

#if TIME_MODELCOMP
uint64_t sum_micros = 0;
uint64_t sum_waitimm = 0;
#endif

#if YCSB_LOAD_THREAD
bool only_load = false;
#endif

#if SST_LIFESPAN
std::mutex mutex_lifespan_;
std::unordered_map<uint64_t, FileLifespanData, hash_FileLifespanData> lifespans;
#endif

#if AC_TEST
uint64_t num_files_flush = 0;
uint64_t num_files_compaction = 0;
uint64_t num_learned = 0;
uint64_t num_merged = 0;
uint64_t num_retrained = 0;
std::atomic<uint32_t> num_tryretraining = 0;
#if RETRAIN2
std::atomic<uint32_t> num_erroradded = 0;
#endif
/*bool count_compaction_triggered_after_load = false;
uint64_t time_compaction_triggered_after_load = 0;
uint32_t num_compaction_triggered_after_load = 0;
uint32_t num_inputs_compaction_triggered_after_load = 0;		// input files
uint32_t num_outputs_compaction_triggered_after_load = 0;	// output files
int64_t size_inputs_compaction_triggered_after_load = 0;				// input files
int64_t size_outputs_compaction_triggered_after_load = 0;			// output files*/

std::atomic<uint64_t> file_size[7];		// file size per level
std::atomic<uint64_t> num_files[7];
#endif

#if AC_TEST2
PaddedAtomic served_i_time[6];
PaddedAtomic served_i[6];
PaddedAtomic served_l_time[6];
PaddedAtomic served_l[6];
PaddedAtomic served_m_time[4];
PaddedAtomic served_m[4];
PaddedAtomic served_r_time[4];
PaddedAtomic served_r[4];

std::atomic<uint32_t> merged_model_miss(0);
std::atomic<uint32_t> served_m_linear(0);
std::atomic<uint32_t> served_m_linear_fail(0);

std::atomic<uint64_t> linear_time(0);
std::atomic<uint32_t> linear_num(0);
#endif

#if AC_TEST3
std::atomic<uint32_t> num_files_compaction_[4];
std::atomic<uint32_t> num_files_learned_[4];
std::atomic<uint32_t> num_files_merged_[4];
#endif

#if TIME_W
uint64_t learntime = 0;
uint32_t num_learntime = 0;
uint64_t mergetime = 0;
uint32_t num_mergetime = 0;
//uint64_t compactiontime[2][5];
//uint32_t num_compactiontime[2][5];
uint64_t compactiontime2[2][7];
uint32_t num_compactiontime2[2][7];
uint64_t learntime_l0 = 0;
uint32_t num_learntime_l0 = 0;
uint64_t learn_bytesize = 0;
uint32_t num_learn_bytesize = 0;
uint64_t merge_bytesize = 0;
uint32_t num_merge_bytesize = 0;

uint64_t learn_size = 0;
uint32_t num_learn_size = 0;
uint64_t merge_size = 0;
uint32_t num_merge_size = 0;

int64_t size_inputs[2][7];
int64_t size_outputs[2][7];
uint32_t num_inputs[2][7];
uint32_t num_outputs[2][7];
#endif

#if TIME_R_DETAIL
/*std::atomic<uint32_t> num_mem(0);
std::atomic<uint32_t> num_imm(0);
std::atomic<uint32_t> num_mem_succ(0);
std::atomic<uint32_t> num_imm_succ(0);
std::atomic<uint32_t> num_ver_succ(0);
std::atomic<uint64_t> time_mem(0);
std::atomic<uint64_t> time_imm(0);*/
std::atomic<uint32_t> num_ver(0);
std::atomic<uint64_t> time_ver(0);
std::atomic<uint32_t> num_vlog(0);
std::atomic<uint64_t> time_vlog(0);
#endif

#if TIME_R
std::atomic<uint32_t> num_i_path(0);
std::atomic<uint32_t> num_l_path(0);
std::atomic<uint32_t> num_m_path(0);
std::atomic<uint64_t> i_path(0);
std::atomic<uint64_t> l_path(0);
std::atomic<uint64_t> m_path(0);
#endif
#if TIME_R_LEVEL
std::atomic<uint32_t> num_i_path_l[7];
std::atomic<uint32_t> num_l_path_l[7];
std::atomic<uint32_t> num_m_path_l[7];
std::atomic<uint64_t> i_path_l[7];
std::atomic<uint64_t> l_path_l[7];
std::atomic<uint64_t> m_path_l[7];
/*uint32_t num_i_path_l[7];
uint32_t num_l_path_l[7];
uint32_t num_m_path_l[7];
uint64_t i_path_l[7];
uint64_t l_path_l[7];
uint64_t m_path_l[7];*/
#endif

#if MULTI_COMPACTION_CNT
uint64_t num_PickCompactionLevel = 0;
uint64_t num_PickCompaction = 0;
uint64_t num_compactions = 0;
uint64_t num_output_files = 0;
#endif

#if MODELCOMP_TEST
std::atomic<uint64_t> num_inserts[2];
std::atomic<uint64_t> num_comparisons[2];
std::atomic<uint64_t> num_keys_lower[2];
std::atomic<uint64_t> num_keys_upper[2];
std::atomic<uint64_t> total_num_keys[2];

std::atomic<uint64_t> total_cpu_cycles[2];
std::atomic<uint32_t> num_cpu_cycles[2];
uint64_t rdtsc_start() {
	unsigned int dummy;
	//__asm__ volatile("cpuid" : : : "%rax", "%rbx", "%rcx", "%rdx");
  return __rdtsc();
}
uint64_t rdtsc_end() {
	unsigned int dummy;
  uint64_t t = __rdtscp(&dummy);
	//__asm__ volatile("cpuid" : : : "%rax", "%rbx", "%rcx", "%rdx");
  return t;
}
#endif

void Report() {
#if MODELCOMP_TEST
	std::cout << "----------------------------------------------------------" << std::endl;
	for (int i=0; i<2; i++) {
		std::cout << "[ L" << i+1 << "->L" << i+2 << " Compaction Using Merged Models ]\n";
		std::cout << "\tTotal number of input keys (lower level): " << koo::num_keys_lower[i] << "\n";
		std::cout << "\tTotal number of input keys (upper level): " << koo::num_keys_upper[i] << "\n";
		std::cout << "\tTotal number of input keys:\t\t\t\t" << koo::total_num_keys[i] << "\n";
		std::cout << "\tTotal number of key insertions:\t\t" << koo::num_inserts[i] << "\n";
		std::cout << "\tTotal number of key comparisons:\t\t" << koo::num_comparisons[i] << "\n";
		/*std::cout << "\tTotal CPU cycles:\t" << koo::total_cpu_cycles[i] << ", num:\t" << koo::num_cpu_cycles[i] << "\n";
		std::cout << "\tAverage CPU cycles:\t" << std::to_string(koo::total_cpu_cycles[i]/((double)koo::num_cpu_cycles[i])) << "\n";*/
	}
#endif
#if AC_TEST
	std::string stat_str;
	koo::db->GetProperty("leveldb.stats", &stat_str);
	printf("\n%s\n", stat_str.c_str());
	if (koo::num_files_flush || koo::num_files_compaction || koo::num_learned || koo::num_merged) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# generated files by flush = " << koo::num_files_flush << std::endl;
		std::cout << "# generated files by compaction = " << koo::num_files_compaction << std::endl;
		std::cout << "# learned files = " << koo::num_learned << std::endl;
		std::cout << "# merged files = " << koo::num_merged << std::endl;
		std::cout << "# retrained files = " << koo::num_retrained << std::endl;
#if RETRAIN2
		std::cout << "# try retraining files = " << koo::num_tryretraining << std::endl;
		std::cout << "# merged models error extended = " << koo::num_erroradded << std::endl;
#endif
		/*std::cout << "\n# input files of compaction triggered after load = " << koo::num_inputs_compaction_triggered_after_load << ",\tTotal size = " << koo::size_inputs_compaction_triggered_after_load << std::endl;
		std::cout << "# output files of compaction triggered after load = " << koo::num_outputs_compaction_triggered_after_load << ",\tTotal size = " << koo::size_outputs_compaction_triggered_after_load << std::endl;
		std::cout << "# compaction triggered after load = " << koo::num_compaction_triggered_after_load << std::endl;
		std::cout << "Avg compaction time triggered after load = " << std::to_string((double)koo::time_compaction_triggered_after_load/((double)koo::num_compaction_triggered_after_load)) << " ns,\ttotal = " << koo::time_compaction_triggered_after_load << std::endl;*/
		for (int i=0; i<7; i++) {
			if (num_files[i] == 0) continue;
			std::cout << "Level " << i << " avg file size: " << std::to_string(file_size[i]/((double)num_files[i])) << " B (num: " << num_files[i] << ")\n";
		}
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_MODELCOMP
	std::cout << "----------------------------------------------------------" << std::endl;
	std::cout << "Sum micros: " << koo::sum_micros/1000 << std::endl;
	std::cout << "Sum wait imm: " << koo::sum_waitimm << std::endl;
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if MODEL_BREAKDOWN
	for (int i=0; i<7; i++) {
		if (!(lm_num[i] || mm_num[i])) continue;
		std::cout << "[ Level " << i << " ]\n";
		if (lm_num[i]) {
			std::cout << "\t# of segs in learned model - \tAVG: " << std::to_string(lm_segs[i]/(double)lm_num[i]) << ",\tnum: " << lm_num[i] << ",\ttotal: " << lm_segs[i] << "\n";
			std::cout << "\t# of keys in learned model - \tAVG: " << std::to_string(lm_keys[i]/(double)lm_num[i]) << ",\tnum: " << lm_num[i] << ",\ttotal: " << lm_keys[i] << "\n";
		}
		if (mm_num[i]) {
			std::cout << "\t# of segs in merged model - \tAVG: " << std::to_string(mm_segs[i]/(double)mm_num[i]) << ",\tnum: " << mm_num[i] << ",\ttotal: " << mm_segs[i] << "\n";
			std::cout << "\t# of keys in merged model - \tAVG: " << std::to_string(mm_keys[i]/(double)mm_num[i]) << ",\tnum: " << mm_num[i] << ",\ttotal: " << mm_keys[i] << "\n";
		}
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if LOOKUP_ACCURACY
	for (int i=0; i<7; i++) {
		if (!(lm_num_error[i] || mm_num_error[i] || rm_num_error[i])) continue;
		std::cout << "[ Level " << i << " ]\n";
		uint64_t total_error = 0, total_num_error = 0;
		if (lm_num_error[i]) {
			total_error += lm_error[i];
			total_num_error += lm_num_error[i];
			std::cout << "\tLearned Model\n";
			std::cout << "\t\tAvg. error when lookup: " << std::to_string(lm_error[i]/(double)lm_num_error[i]);
			std::cout << "\t\tNum: " << lm_num_error[i] << ",\ttotal error: " << lm_error[i] << std::endl;
		}
		if (mm_num_error[i]) {
			total_error += mm_error[i];
			total_num_error += mm_num_error[i];
			std::cout << "\tMerged Model\n";
			std::cout << "\t\tAvg. error when lookup: " << std::to_string(mm_error[i]/(double)mm_num_error[i]);
			std::cout << "\t\tNum: " << mm_num_error[i] << ",\ttotal error: " << mm_error[i] << std::endl;
		}
		if (rm_num_error[i]) {
			total_error += rm_error[i];
			total_num_error += rm_num_error[i];
			std::cout << "\tRetrained Model\n";
			std::cout << "\t\tAvg. error when lookup: " << std::to_string(rm_error[i]/(double)rm_num_error[i]);
			std::cout << "\t\tNum: " << rm_num_error[i] << ",\ttotal error: " << rm_error[i] << std::endl;
		}
		std::cout << "\tTotal Model Lookup Error\n";
		std::cout << "\t\tAvg. error when lookup: " << std::to_string(total_error/(double)total_num_error);
		std::cout << "\t\tNum: " << total_num_error << ",\ttotal error: " << total_error << std::endl;
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if MODEL_ACCURACY
	for (int i=0; i<7; i++) {
		if (!(lm_num_error2[i] || mm_num_error2[i])) continue;
		std::cout << "[ Level " << i << " ]\n";
		if (lm_num_error2[i]) {
			std::cout << "Learned Model\n";
			std::cout << "\tNum: " << lm_num_error2[i] << ",\ttotal avg_error: " << lm_avg_error[i] << ",\ttotal max_error: " << lm_max_error[i] << std::endl;
			std::cout << "\tAvg. avg_error: " << std::to_string(lm_avg_error[i]/(double)lm_num_error2[i]);
			std::cout << ",\tAvg. max_error: " << std::to_string(lm_max_error[i]/(double)lm_num_error2[i]) << std::endl;
		}
		if (mm_num_error2[i]) {
			std::cout << "Merged Model\n";
			std::cout << "\tNum: " << mm_num_error2[i] << ",\ttotal avg_error: " << mm_avg_error[i] << ",\ttotal max_error: " << mm_max_error[i] << std::endl;
			std::cout << "\tAvg. avg_error: " << std::to_string(mm_avg_error[i]/(double)mm_num_error2[i]);
			std::cout << ",\tAvg. max_error: " << std::to_string(mm_max_error[i]/(double)mm_num_error2[i]) << std::endl;
			std::cout << "\t--Over error bound--\n";
			std::cout << "\tNum: " << mm_num_error_over[i] << ",\ttotal avg_error: " << mm_avg_error_over[i] << ",\ttotal max_error: " << mm_max_error_over[i] << std::endl;
			std::cout << "\tAvg. avg_error: " << std::to_string(mm_avg_error_over[i]/(double)mm_num_error_over[i]);
			std::cout << ",\tAvg. max_error: " << std::to_string(mm_max_error_over[i]/(double)mm_num_error_over[i]) << std::endl;
			std::cout << "\tAvg. cnt_overmax: " << std::to_string(mm_cnt_overmax[i]/(double)mm_num_error_over[i]) << ",\ttotal cnt_overmax: " << mm_cnt_overmax[i] << std::endl;
		}
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if AC_TEST2
	std::cout << "----------------------------------------------------------" << std::endl;
	uint64_t total_served_i_time = 0, total_served_l_time = 0, total_served_m_time = 0, total_served_r_time = 0;
	uint64_t total_served_i = 0, total_served_l = 0, total_served_m = 0, total_served_r = 0;
	for (int l=0; l<6; l++) {
		if (!(served_i[l].val || served_l[l].val)) {
			if (l < 2) continue;
			if (!(served_m[l-2].val || served_r[l-2].val)) continue;
		}
		std::cout << "[ Level " << l << " ]\n";
		if (served_i[l].val) {
			std::cout << "\tAvg baseline path time: " << std::to_string(served_i_time[l].val/(double)served_i[l].val);
			std::cout << ",\tnum: " << served_i[l].val << ",\ttotal: " << served_i_time[l].val << std::endl;
		}
		if (served_l[l].val) {
			std::cout << "\tAvg learned model path time: " << std::to_string(served_l_time[l].val/(double)served_l[l].val);
			std::cout << ",\tnum: " << served_l[l].val << ",\ttotal: " << served_l_time[l].val << std::endl;
		}
		total_served_i_time += served_i_time[l].val;
		total_served_l_time += served_l_time[l].val;
		total_served_i += served_i[l].val;
		total_served_l += served_l[l].val;
		if (l > 1) {
			if (served_m[l-2].val) {
				std::cout << "\tAvg merged model path time: " << std::to_string(served_m_time[l-2].val/(double)served_m[l-2].val);
				std::cout << ",\tnum: " << served_m[l-2].val << ",\ttotal: " << served_m_time[l-2].val << std::endl;
			}
			if (served_r[l-2].val) {
				std::cout << "\tAvg retrained model path time: " << std::to_string(served_r_time[l-2].val/(double)served_r[l-2].val);
				std::cout << ",\tnum: " << served_r[l-2].val << ",\ttotal: " << served_r_time[l-2].val << std::endl;
			}
			total_served_m_time += served_m_time[l-2].val;
			total_served_r_time += served_r_time[l-2].val;
			total_served_m += served_m[l-2].val;
			total_served_r += served_r[l-2].val;
		}
	}
	if (total_served_i || total_served_l || total_served_m || total_served_r) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Index block path avg time: " << std::to_string(total_served_i_time/((double)total_served_i)) << " ns,\t";
		std::cout << "(num: " << total_served_i << ",\ttotal: " << total_served_i_time << " ns)\n";
		std::cout << "Learned model path avg time: " << std::to_string(total_served_l_time/((double)total_served_l)) << " ns,\t";
		std::cout << "(num: " << total_served_l << ",\ttotal: " << total_served_l_time << " ns)\n";
		std::cout << "Merged model path avg time: " << std::to_string(total_served_m_time/((double)total_served_m)) << " ns,\t";
		std::cout << "(num: " << total_served_m << ",\ttotal: " << total_served_m_time << " ns)\n";
		std::cout << "Retrained model path avg time: " << std::to_string(total_served_r_time/((double)total_served_r)) << " ns,\t";
		std::cout << "(num: " << total_served_r << ",\ttotal: " << total_served_r_time << " ns)\n";
		std::cout << "----------------------------------------------------------" << std::endl;
		uint64_t total_served = total_served_i + total_served_l + total_served_m + total_served_r;
		uint64_t total_served_model = total_served_l + total_served_m + total_served_r;
		std::cout << "Total # of SST lookups: " << total_served << "\n";
		std::cout << "# of IB path / total: " << std::to_string(total_served_i/(double)total_served) << ",\t";
		std::cout << "# of model path / total: " << std::to_string(total_served_model/(double)total_served) << "\n";
		std::cout << "\t# of LM path / # of model path: " << std::to_string(total_served_l/(double)total_served_model) << "\n";
		std::cout << "\t# of MM path / # of model path: " << std::to_string(total_served_m/(double)total_served_model) << "\n";
		std::cout << "\t# of RM path / # of model path: " << std::to_string(total_served_r/(double)total_served_model) << "\n";
			std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "\nAvg merged model linear search time:\t" << std::to_string(linear_time/(double)linear_num);
		std::cout << ", num: " << linear_num << ", total: " << linear_time << std::endl;
		std::cout << "# Merged model missed:\t" << merged_model_miss << std::endl;
		std::cout << "# Merged model linear search served:\t " << served_m_linear + served_m_linear_fail;
		std::cout << " (succeed: " << served_m_linear << ", failed: " << served_m_linear_fail << ")\n";
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if AC_TEST3
	if (koo::num_files_compaction_[1]) {
		std::cout << "----------------------------------------------------------" << std::endl;
		for (int i=0; i<4; i++) {
			std::cout << "[ Level " << i << " ]\n";
			std::cout << "\t# compaction files: " << koo::num_files_compaction_[i] << std::endl;
			std::cout << "\t# learned files: " << koo::num_files_learned_[i] << std::endl;
			std::cout << "\t# merged files: " << koo::num_files_merged_[i] << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_W
	if (koo::num_learntime || koo::num_compactiontime2[0][1] || koo::num_compactiontime2[1][2] || koo::num_mergetime) {
		/*std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "DoCompactionWork() function totally\n";
		for (int i=0; i<5; i++) {
			if (!(koo::num_compactiontime[0][i] || koo::num_compactiontime[1][i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			std::cout << "\tw/o merging - avg compaction time:\t" << std::to_string(koo::compactiontime[0][i]/((double)koo::num_compactiontime[0][i])) << " ns, num:\t" << koo::num_compactiontime[0][i] << ", Total compaction time:\t" << koo::compactiontime[0][i]/1000000 << " ms\n";
			std::cout << "\tw/  merging - avg compaction time:\t" << std::to_string(koo::compactiontime[1][i]/((double)koo::num_compactiontime[1][i])) << " ns, num:\t" << koo::num_compactiontime[1][i] << ", Total compaction time:\t" << koo::compactiontime[1][i]/1000000 << " ms\n";
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "DoCompactionWork() function internal partly\n";
		for (int i=0; i<5; i++) {
			if (!(koo::num_compactiontime2[0][i] || koo::num_compactiontime2[1][i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			std::cout << "\tw/o merging - avg compaction time:\t" << std::to_string(koo::compactiontime2[0][i]/((double)koo::num_compactiontime2[0][i])) << " ns, num:\t" << koo::num_compactiontime2[0][i] << ", Total compaction time:\t" << koo::compactiontime2[0][i]/1000000 << " ms\n";
			std::cout << "\tw/  merging - avg compaction time:\t" << std::to_string(koo::compactiontime2[1][i]/((double)koo::num_compactiontime2[1][i])) << " ns, num:\t" << koo::num_compactiontime2[1][i] << ", Total compaction time:\t" << koo::compactiontime2[1][i]/1000000 << " ms\n";
		}*/
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Compaciton input/output files size\n";
		for (int i=0; i<7; i++) {
			if (!(koo::num_compactiontime2[0][i] || koo::num_compactiontime2[1][i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			std::cout << "\tw/o merging - num:\t" << koo::num_compactiontime2[0][i] << ", total compaction time:\t" << koo::compactiontime2[0][i]/1000000 << " ms\n";
			if (koo::num_compactiontime2[0][i]) {
				std::cout << "\t - Total input size: \t" << koo::size_inputs[0][i] << " B, avg inputs size per compaction time: \t" << std::to_string(koo::size_inputs[0][i]*1000000/((double)koo::compactiontime2[0][i])) << " B/ms\n";
				std::cout << "\t - Total output size:\t" << koo::size_outputs[0][i] << " B, avg outputs size per compaction time:\t" << std::to_string(koo::size_outputs[0][i]*1000000/((double)koo::compactiontime2[0][i])) << " B/ms\n";
			}
			std::cout << "\tw/  merging - num:\t" << koo::num_compactiontime2[1][i] << ", total compaction time:\t" << koo::compactiontime2[1][i]/1000000 << " ms\n";
			if (koo::num_compactiontime2[1][i]) {
				std::cout << "\t - Total input size: \t" << koo::size_inputs[1][i] << " B, avg inputs size per compaction time: \t" << std::to_string(koo::size_inputs[1][i]*1000000/((double)koo::compactiontime2[1][i])) << " B/ms\n";
				std::cout << "\t - Total output size:\t" << koo::size_outputs[1][i] << " B, avg outputs size per compaction time:\t" << std::to_string(koo::size_outputs[1][i]*1000000/((double)koo::compactiontime2[1][i])) << " B/ms\n";
			}
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		//std::cout << "Avg compaction time = " << std::to_string((double)koo::compactiontime/(double)koo::num_compactiontime) << " ns,\t# compactions = " << koo::num_compactiontime << ",\tTotal compaction time = " << koo::compactiontime << std::endl;
#if AC_TEST
		//std::cout << "Avg compaction time per file = " << std::to_string((double)koo::compactiontime/(double)koo::num_files_compaction) << " ns\n";
#endif
		std::cout << "Avg learning time per file = " << std::to_string((double)koo::learntime/(double)koo::num_learntime) << " ns,\t# learned files = " << koo::num_learntime << ",\tTotal learning time = " << std::to_string(koo::learntime) << std::endl;
		std::cout << "Avg learning time per L0 file = " << std::to_string((double)koo::learntime_l0/(double)koo::num_learntime_l0) << " ns,\t# learned L0 files = " << koo::num_learntime_l0 << ",\tTotal L0 files learning time = " << std::to_string(koo::learntime_l0) << std::endl;
		std::cout << "Avg merging time per compaction = " << std::to_string((double)koo::mergetime/(double)koo::num_mergetime) << " ns,\t# compactions with merging = " << koo::num_mergetime << ",\tTotal merging time = " << std::to_string(koo::mergetime) << " ns\n";
		std::cout << "\tAvg merging time per file = " << std::to_string((double)koo::mergetime/(double)koo::num_merge_size) << " ns,\t# merged files = " << koo::num_merge_size << std::endl;
		std::cout << "Avg # of items to learn = " << std::to_string((double)koo::learn_size/(double)koo::num_learn_size) << ",\tnum = " << koo::num_learn_size << std::endl;
		std::cout << "Avg # of items to merge = " << std::to_string((double)koo::merge_size/(double)koo::num_merge_size) << ",\tnum = " << koo::num_merge_size << std::endl;
		std::cout << "Total learned bytes: " << koo::learn_bytesize << " B,\tnum: " << koo::num_learn_bytesize << std::endl;
		std::cout << "Total merged bytes: " << koo::merge_bytesize << " B,\tnum: " << koo::num_merge_bytesize << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_R
	if (koo::num_i_path || koo::num_l_path || koo::num_m_path) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Index block path avg time = " << std::to_string(std::round(koo::i_path/((double)koo::num_i_path))) << " ns,\t# index block path = " << koo::num_i_path << ",\tTotal time = " << std::to_string(koo::i_path) << std::endl;
		std::cout << "Learned model path avg time = " << std::to_string(std::round(koo::l_path/((double)koo::num_l_path))) << " ns,\t# learned model path = " << koo::num_l_path << ",\tTotal time = " << std::to_string(koo::l_path) << std::endl;
		std::cout << "Merged model path avg time = " << std::to_string(std::round(koo::m_path/((double)koo::num_m_path))) << " ns,\t# merged model path = " << koo::num_m_path << ",\tTotal time = " << std::to_string(koo::m_path) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_R_LEVEL
	if (koo::num_i_path_l[0] || koo::num_l_path_l[0] || koo::num_l_path_l[1] || koo::num_i_path_l[2] || koo::num_m_path_l[2]) {
		std::cout << "----------------------------------------------------------" << std::endl;
		uint64_t total_num_i_path = 0, total_num_l_path = 0, total_num_m_path = 0;
		uint64_t total_i_path = 0, total_l_path = 0, total_m_path = 0;
		for (unsigned l=0; l<7; l++) {
			if (koo::num_i_path_l[l] == 0 && koo::num_l_path_l[l] == 0 && koo::num_m_path_l[l] == 0) continue;
			std::cout << "[ Level " << l << " ]\n";
			std::cout << "\tIndex block path avg time = " << std::to_string(koo::i_path_l[l]/((double)koo::num_i_path_l[l])) << " ns,\t# index block path = " << koo::num_i_path_l[l] << ",\tTotal time = " << std::to_string(koo::i_path_l[l]) << std::endl;
			std::cout << "\tLearned model path avg time = " << std::to_string(koo::l_path_l[l]/((double)koo::num_l_path_l[l])) << " ns,\t# learned model path = " << koo::num_l_path_l[l] << ",\tTotal time = " << std::to_string(koo::l_path_l[l]) << std::endl;
			std::cout << "\tMerged model path avg time = " << std::to_string(koo::m_path_l[l]/((double)koo::num_m_path_l[l])) << " ns,\t# merged model path = " << koo::num_m_path_l[l] << ",\tTotal time = " << std::to_string(koo::m_path_l[l]) << std::endl;
			total_num_i_path += koo::num_i_path_l[l];
			total_num_l_path += koo::num_l_path_l[l];
			total_num_m_path += koo::num_m_path_l[l];
			total_i_path += koo::i_path_l[l];
			total_l_path += koo::l_path_l[l];
			total_m_path += koo::m_path_l[l];
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Index block path avg time = " << std::to_string(total_i_path/((double)total_num_i_path)) << " ns,\t# index block path = " << total_num_i_path << ",\tTotal time = " << std::to_string(total_i_path) << std::endl;
		std::cout << "Learned model path avg time = " << std::to_string(total_l_path/((double)total_num_l_path)) << " ns,\t# learned model path = " << total_num_l_path << ",\tTotal time = " << std::to_string(total_l_path) << std::endl;
		std::cout << "Merged model path avg time = " << std::to_string(total_m_path/((double)total_num_m_path)) << " ns,\t# merged model path = " << total_num_m_path << ",\tTotal time = " << std::to_string(total_m_path) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
		uint64_t total_num_path = total_num_i_path + total_num_l_path + total_num_m_path;
		uint64_t total_num_model_path = total_num_l_path + total_num_m_path;
		uint64_t total_time = (total_i_path + total_l_path + total_m_path)/1000000000;
		std::cout << "Total # path: " << total_num_path << ", total time: " << total_time << " s\n";
		std::cout << "# IB path / total = " << std::to_string(total_num_i_path/(double)total_num_path) << ",\t# model path / total = " << std::to_string(total_num_model_path/(double)total_num_path) << std::endl;
		std::cout << "# learned model path / # model path = " << std::to_string(total_num_l_path/(double)total_num_model_path) << ",\t# merged model path / # model path = " << std::to_string(total_num_m_path/(double)total_num_model_path) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_R_DETAIL
	if (/*koo::num_mem ||*/ koo::num_ver) {
		std::cout << "----------------------------------------------------------" << std::endl;
		//std::cout << "[ MEM ]\nTotal num: " << koo::num_mem << ",\tFound num: " << koo::num_mem_succ << ",\tTotal time: " << std::to_string(koo::time_mem) << " ns,\tAVG: " << std::to_string(koo::time_mem/((double)koo::num_mem)) << " ns" << std::endl;
		//std::cout << "[ IMM ]\nTotal num: " << koo::num_imm << ",\tFound num: " << koo::num_imm_succ << ",\tTotal time: " << std::to_string(koo::time_imm) << " ns,\tAVG: " << std::to_string(koo::time_imm/((double)koo::num_imm)) << " ns" << std::endl;
		//std::cout << "[ VER ]\nTotal num: " << koo::num_ver << ",\tFound num: " << koo::num_ver_succ << ",\tTotal time: " << std::to_string(koo::time_ver) << " ns,\tAVG: " << std::to_string(koo::time_ver/((double)koo::num_ver)) << " ns" << std::endl;
		std::cout << "[ VER ]\nTotal num: " << koo::num_ver << ",\tTotal time: " << std::to_string(koo::time_ver) << " ns,\tAVG: " << std::to_string(koo::time_ver/((double)koo::num_ver)) << " ns" << std::endl;
		std::cout << "[ VLOG ]\nTotal num: " << koo::num_vlog << ",\tTotal time: " << std::to_string(koo::time_vlog) << " ns,\tAVG: " << std::to_string(koo::time_vlog/((double)koo::num_vlog)) << " ns" << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if MULTI_COMPACTION_CNT
	if (koo::num_PickCompaction) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# PickCompactionLevel() called: " << koo::num_PickCompactionLevel << std::endl;
		std::cout << "# PickCompaction() called: " << koo::num_PickCompaction << std::endl;
		std::cout << "# Compactions: " << koo::num_compactions << std::endl;
		std::cout << "# Compaction output files: " << koo::num_output_files << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if SST_LIFESPAN
	if (!koo::lifespans.empty()) {
		std::vector<std::pair<uint64_t, koo::FileLifespanData>> lss[config::kNumLevels];
		for (auto& ls : koo::lifespans) lss[ls.second.level].push_back(std::make_pair(ls.first, ls.second));
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "중간에 멈춘거, 삭제된거, 학습 안한거 등등 모든 경우 다 합친 레벨별 time\n";
		int64_t total_T_time[config::kNumLevels] = {0, };
		uint32_t num_T_time[config::kNumLevels] = {0, };
		int64_t total_W_time[config::kNumLevels] = {0, };
		uint32_t num_W_time[config::kNumLevels] = {0, };
		int64_t total_W_time_pos[config::kNumLevels] = {0, };
		uint32_t num_W_time_pos[config::kNumLevels] = {0, };
		int64_t total_M_time[config::kNumLevels] = {0, };
		uint32_t num_M_time[config::kNumLevels] = {0, };
		int64_t  total_U_time[config::kNumLevels] = {0, };
		uint32_t num_U_time[config::kNumLevels] = {0, };
		//std::ofstream ofs("/koo/HyperLearningless/koo/data/lifespans.txt");
		std::string str = "";
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			//ofs << "[ Level " << i << " Tables ] num: " << lss[i].size() << std::endl;
			str += "Level " + to_string(i) + " num: " + to_string(lss[i].size()) + "\n";
			for (auto& ls : lss[i]) {
				//ofs << ls.first << "\t(level " << i << "):\t" << ls.second.T_start << " " << ls.second.T_end << " " << ls.second.M_end << " " << ls.second.U_end << std::endl;
				std::string learned_str = "";
				if (ls.second.learned) learned_str = "1";
				else learned_str = "0";
				std::string merged_str = "";
				if (ls.second.merged) merged_str = "1";
				else merged_str = "0";
				str += to_string(ls.first) + " " + learned_str + " " + merged_str + " " + to_string(ls.second.T_start) + " ";
				str += to_string(ls.second.T_end) + " " + to_string(ls.second.W_end) + " ";
				str += to_string(ls.second.M_end) + " " + to_string(ls.second.U_end) + "\n";

				if (ls.second.T_end != 0) {
					total_T_time[i] += ls.second.T_end - ls.second.T_start;
					num_T_time[i]++;
					if (ls.second.W_end != 0) {
						total_W_time[i] += (int)(ls.second.W_end - ls.second.T_end);
						num_W_time[i]++;
						if ((int)(ls.second.W_end - ls.second.T_end) > 0) {
							total_W_time_pos[i] += (int)(ls.second.W_end - ls.second.T_end);
							num_W_time_pos[i]++;
						}
						if (ls.second.M_end != 0) {			// not L0 files
							total_M_time[i] += (int)(ls.second.M_end - ls.second.W_end);
							num_M_time[i]++;
							if (ls.second.U_end != 0) {			// files built when workload is ending
								total_U_time[i] += ls.second.U_end - ls.second.M_end;
								num_U_time[i]++;
							}
						}
					}
				}
			}
		}
		//ofs.close();
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			std::cout << "[ Level " << i << " Tables ] num: " << lss[i].size() << std::endl;
			std::cout << "\tAvg. T_time: " << std::to_string(total_T_time[i]/(double)num_T_time[i]) << ",\t\tTotal: " << total_T_time[i] << ",\tnum: " << num_T_time[i] << std::endl;
			std::cout << "\tAvg. W_time: " << std::to_string(total_W_time[i]/(double)num_W_time[i]) << ",\t\tTotal: " << total_W_time[i] << ",\tnum: " << num_W_time[i] << std::endl;
			std::cout << "\tAvg. positive W_time: " << std::to_string(total_W_time_pos[i]/(double)num_W_time_pos[i]) << ",\t\tTotal: " << total_W_time_pos[i] << ",\tnum: " << num_W_time_pos[i] << std::endl;
			std::cout << "\tAvg. M_time: " << std::to_string(total_M_time[i]/(double)num_M_time[i]) << ",\t\tTotal: " << total_M_time[i] << ",\tnum: " << num_M_time[i] << std::endl;
			std::cout << "\tAvg. U_time: " << std::to_string(total_U_time[i]/(double)num_U_time[i]) << ",\t\tTotal: " << total_U_time[i] << ",\tnum: " << num_U_time[i] << std::endl;
		}
		/*std::cout << "----------------------------------------------------------" << std::endl;
		int64_t total_T_time[2][config::kNumLevels] = {0, };
		uint32_t num_T_time[2][config::kNumLevels] = {0, };
		int64_t total_M_time[2][config::kNumLevels] = {0, };
		uint32_t num_M_time[2][config::kNumLevels] = {0, };
		int64_t total_M_time_pos[2][config::kNumLevels] = {0, };
		uint32_t num_M_time_pos[2][config::kNumLevels] = {0, };
		int64_t  total_U_time[2][config::kNumLevels] = {0, };
		uint32_t num_U_time[2][config::kNumLevels] = {0, };
		std::vector<std::pair<uint64_t, koo::FileLifespanData>> lss[config::kNumLevels];
		for (auto& ls : koo::lifespans) lss[ls.second.level].push_back(std::make_pair(ls.first, ls.second));
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			for (auto& ls : lss[i]) {
				int m = ls.second.merged ? 1 : 0;
				total_T_time[m][i] += ls.second.T_end - ls.second.T_start;
				num_T_time[m][i]++;
				if (ls.second.M_end != 0) {
					total_M_time[m][i] += (int)(ls.second.M_end - ls.second.T_end);
					num_M_time[m][i]++;
					if ((int)(ls.second.M_end - ls.second.T_end) > 0) {
						total_M_time_pos[m][i] += (int)(ls.second.M_end - ls.second.T_end);
						num_M_time_pos[m][i]++;
					}
					if (ls.second.U_end != 0) {			// files built when workload is ending
						total_U_time[m][i] += ls.second.U_end - ls.second.M_end;
						num_U_time[m][i]++;
					}
				}
			}
		}
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			std::cout << "[ Level " << i << " Tables ] num: " << lss[i].size() << std::endl;
			std::cout << "\t< Learning >\n";
			std::cout << "\t\tAvg. T_time: " << std::to_string(total_T_time[0][i]/(double)num_T_time[0][i]) << ",\t\tTotal: " << total_T_time[0][i] << ",\tnum: " << num_T_time[0][i] << std::endl;
			std::cout << "\t\tAvg. M_time: " << std::to_string(total_M_time[0][i]/(double)num_M_time[0][i]) << ",\t\tTotal: " << total_M_time[0][i] << ",\tnum: " << num_M_time[0][i] << std::endl;
			std::cout << "\t\tAvg. positive M_time: " << std::to_string(total_M_time_pos[0][i]/(double)num_M_time_pos[0][i]) << ",\t\tTotal: " << total_M_time_pos[0][i] << ",\tnum: " << num_M_time_pos[0][i] << std::endl;
			std::cout << "\t\tAvg. U_time: " << std::to_string(total_U_time[0][i]/(double)num_U_time[0][i]) << ",\t\tTotal: " << total_U_time[0][i] << ",\tnum: " << num_U_time[0][i] << std::endl;
			std::cout << "\t< Merging >\n";
			std::cout << "\t\tAvg. T_time: " << std::to_string(total_T_time[1][i]/(double)num_T_time[1][i]) << ",\t\tTotal: " << total_T_time[1][i] << ",\tnum: " << num_T_time[1][i] << std::endl;
			std::cout << "\t\tAvg. M_time: " << std::to_string(total_M_time[1][i]/(double)num_M_time[1][i]) << ",\t\tTotal: " << total_M_time[1][i] << ",\tnum: " << num_M_time[1][i] << std::endl;
			std::cout << "\t\tAvg. positive M_time: " << std::to_string(total_M_time_pos[1][i]/(double)num_M_time_pos[1][i]) << ",\t\tTotal: " << total_M_time_pos[1][i] << ",\tnum: " << num_M_time_pos[1][i] << std::endl;
			std::cout << "\t\tAvg. U_time: " << std::to_string(total_U_time[1][i]/(double)num_U_time[1][i]) << ",\t\tTotal: " << total_U_time[1][i] << ",\tnum: " << num_U_time[1][i] << std::endl;
		}*/
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
		fprintf(stdout, "SST_LIFESPAN\n%sSST_LIFESPAN\n", str.c_str());
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
}

void Reset() {
#if AC_TEST
	num_files_flush = 0;
	num_files_compaction = 0;
	num_learned = 0;
	num_merged = 0;
	num_retrained = 0;
#if RETRAIN2
	num_tryretraining = 0;
	num_erroradded = 0;
#endif
#endif
#if TIME_W
	learntime = 0;
	num_learntime = 0;
	mergetime = 0;
	num_mergetime = 0;
	for (int i=0; i<2; i++) {
		for (int j=0; j<7; j++) {
			//compactiontime[i][j] = 0;
			//num_compactiontime[i][j] = 0;
			compactiontime2[i][j] = 0;
			num_compactiontime2[i][j] = 0;
			size_inputs[i][j] = 0;
			num_inputs[i][j] = 0;
			size_outputs[i][j] = 0;
			num_outputs[i][j] = 0;
		}
	}
	learntime_l0 = 0;
	num_learntime_l0 = 0;
	learn_size = 0;
	num_learn_size = 0;
	merge_size = 0;
	num_merge_size = 0;
#endif
#if TIME_R_LEVEL
	for (int i=0; i<7; i++) {
		num_i_path_l[i] = 0;
		num_l_path_l[i] = 0;
		num_m_path_l[i] = 0;
		i_path_l[i] = 0;
		l_path_l[i] = 0;
		m_path_l[i] = 0;
	}
#endif
}
}
