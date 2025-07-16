#include "koo/koo.h"
#include "koo/util.h"

using std::to_string;

namespace koo {

std::string model_dbname = "/models";

#if LOOKUP_ACCURACY
std::atomic<uint64_t> lm_error[7];
std::atomic<uint64_t> lm_num_error[7];
#endif

#if MODEL_ACCURACY
std::atomic<uint64_t> lm_max_error[7];
std::atomic<uint64_t> lm_avg_error[7];
std::atomic<uint64_t> lm_num_error[7];
#endif

#if MODEL_BREAKDOWN
std::atomic<uint64_t> lm_segs[7];	// learned model # of segs
std::atomic<uint64_t> lm_keys[7];	// learned model # of segs
std::atomic<uint64_t> lm_num[7];

uint64_t num_lm[7];		// # of learned models
#endif

#if THREADSAFE
std::condition_variable cv;
std::mutex cv_mtx;
std::atomic<bool> should_stop{false};
#endif

#if TIME_MODELCOMP
uint64_t sum_micros = 0;
uint64_t sum_waittime = 0;
//std::atomic<uint64_t> sum_read(0);
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
bool count_compaction_triggered_after_load = false;
uint64_t time_compaction_triggered_after_load = 0;
uint32_t num_compaction_triggered_after_load = 0;
uint32_t num_inputs_compaction_triggered_after_load = 0;			// input files
uint32_t num_outputs_compaction_triggered_after_load = 0;		// output files
int64_t size_inputs_compaction_triggered_after_load = 0;			// input files
int64_t size_outputs_compaction_triggered_after_load = 0;		// output files
#if TIME_W_DETAIL
uint64_t compactiontime_d[5];
uint32_t num_compactiontime_d[5];
uint64_t bc_d[5];
uint32_t num_bc_d[5];

uint64_t time_filldata;
uint32_t num_filldata;
#endif
#if MC_DEBUG
/*uint32_t id_twait = 0;
uint64_t time_twait[8];
uint32_t num_twait[8];*/
std::atomic<uint64_t> time_tAppend(0);
std::atomic<uint32_t> num_tAppend(0);
#endif
#endif

#if TIME_W
uint64_t learntime = 0;
uint32_t num_learntime = 0;
uint64_t compactiontime[5];
uint32_t num_compactiontime[5];
uint64_t compactiontime2[5];
uint32_t num_compactiontime2[5];

uint64_t learn_size = 0;
uint32_t num_learn_size = 0;

int64_t size_inputs[5];
int64_t size_outputs[5];
uint32_t num_inputs[5];
uint32_t num_outputs[5];
#endif

#if TIME_R
std::atomic<uint32_t> num_i_path(0);
std::atomic<uint32_t> num_m_path(0);
std::atomic<uint64_t> i_path(0);
std::atomic<uint64_t> m_path(0);
#endif
#if TIME_R_DETAIL
std::atomic<uint32_t> num_ver(0);
std::atomic<uint64_t> time_ver(0);
std::atomic<uint32_t> num_vlog(0);
std::atomic<uint64_t> time_vlog(0);
#endif
#if TIME_R_LEVEL
std::atomic<uint32_t> num_i_path_l[7];
std::atomic<uint32_t> num_m_path_l[7];
std::atomic<uint64_t> i_path_l[7];
std::atomic<uint64_t> m_path_l[7];
#endif

#if MULTI_COMPACTION_CNT
uint64_t num_PickCompactionLevel = 0;
uint64_t num_PickCompaction = 0;
uint64_t num_compactions = 0;
uint64_t num_output_files = 0;
#endif

#if MODELCOMP_TEST
std::atomic<uint64_t> num_comparisons[2];
//std::atomic<uint64_t> num_keys_lower[2];
//std::atomic<uint64_t> num_keys_upper[2];
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
		std::cout << "[ L" << i+1 << "->L" << i+2 << " Baseline Compaction ]\n";
		/*//std::cout << "\tTotal number of input keys (lower level): " << koo::num_keys_lower[i] << "\n";
		//std::cout << "\tTotal number of input keys (upper level): " << koo::num_keys_upper[i] << "\n";
		std::cout << "\tTotal number of input keys:\t\t\t\t" << koo::total_num_keys[i] << "\n";
		std::cout << "\tTotal number of key comparisons:\t\t" << koo::num_comparisons[i] << "\n";*/
		std::cout << "\tTotal CPU cycles:\t" << koo::total_cpu_cycles[i] << ", num:\t" << koo::num_cpu_cycles[i] << "\n";
		std::cout << "\tAverage CPU cycles:\t" << std::to_string(koo::total_cpu_cycles[i]/((double)koo::num_cpu_cycles[i])) << "\n";
	}
#endif
#if TIME_MODELCOMP
	std::cout << "----------------------------------------------------------" << std::endl;
	std::cout << "Sum micros: " << koo::sum_micros/1000 << std::endl;
	std::cout << "Sum wait imm: " << koo::sum_waittime << std::endl;
	//std::cout << "Sum read: " << koo::sum_read << std::endl;
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if AC_TEST
	std::string stat_str;
	koo::db->GetProperty("leveldb.stats", &stat_str);
	printf("\n%s\n", stat_str.c_str());
	if (koo::num_files_flush || koo::num_files_compaction || koo::num_learned) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "# generated files by flush = " << koo::num_files_flush << std::endl;
		std::cout << "# generated files by compaction = " << koo::num_files_compaction << std::endl;
		std::cout << "# learned files = " << koo::num_learned << std::endl;
		/*std::cout << "\n# input files of compaction triggered after load = " << koo::num_inputs_compaction_triggered_after_load << ",\tTotal size = " << koo::size_inputs_compaction_triggered_after_load << std::endl;
		std::cout << "# output files of compaction triggered after load = " << koo::num_outputs_compaction_triggered_after_load << ",\tTotal size = " << koo::size_outputs_compaction_triggered_after_load << std::endl;
		std::cout << "# compaction triggered after load = " << koo::num_compaction_triggered_after_load << std::endl;
		std::cout << "Avg compaction time triggered after load = " << std::to_string((double)koo::time_compaction_triggered_after_load/((double)koo::num_compaction_triggered_after_load)) << " ns,\ttotal = " << koo::time_compaction_triggered_after_load << std::endl;*/
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#if TIME_W_DETAIL
	if (koo::num_compactiontime_d[0] || koo::num_compactiontime_d[1]) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Compaction info triggered after load\n";
		for (int i=0; i<5; i++) {
			if (!(koo::num_compactiontime_d[i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			if (koo::num_bc_d[i])
				std::cout << "\tavg BackgroundCompaction() time: " << std::to_string(koo::bc_d[i]/((double)koo::num_bc_d[i])) << " ns,\tnum: " << koo::num_bc_d[i] << std::endl;
			std::cout << "\tavg compaction time: " << std::to_string(koo::compactiontime_d[i]/((double)koo::num_compactiontime_d[i])) << " ns,\tnum: " << koo::num_compactiontime_d[i] << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "FillData avg time: " << std::to_string(koo::time_filldata/(double)koo::num_filldata) << ",\tnum: " << koo::num_filldata << ",\ttotal: " << koo::time_filldata << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if MC_DEBUG
	//if (koo::num_twait[0] || koo::num_twait[1]) {
	if (koo::num_tAppend) {
		std::cout << "----------------------------------------------------------" << std::endl;
		/*std::cout << "Time waiting for compaction per thread\n";
		for (int i=0; i<8; i++) {
			std::cout << "[Thread " << i << "] num: " << koo::num_twait[i] << ",\ttotal: " << koo::time_twait[i] << ",\tavg: " << std::to_string(koo::time_twait[i]/(double)koo::num_twait[i]) << std::endl;
		}*/
		std::cout << "Total time spent on AddRecord: " << koo::time_tAppend << ",\tnum: " << koo::num_tAppend << std::endl;
		std::cout << "Avg: " << std::to_string(koo::time_tAppend/(double)koo::num_tAppend) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#endif
#if LOOKUP_ACCURACY
	for (int i=0; i<7; i++) {
		if (!(lm_num_error[i])) continue;
		std::cout << "[ Level " << i << " ]\n";
		if (lm_num_error[i]) {
			std::cout << "Learned Model\n";
			std::cout << "\tAvg. error when lookup: " << std::to_string(lm_error[i]/(double)lm_num_error[i]);
			std::cout << "\tNum: " << lm_num_error[i] << ",\ttotal error: " << lm_error[i] << std::endl;
		}
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if MODEL_ACCURACY
	for (int i=0; i<7; i++) {
		if (!(lm_num_error[i])) continue;
		std::cout << "[ Level " << i << " ]\n";
		if (lm_num_error[i]) {
			std::cout << "Learned Model\n";
			std::cout << "\tNum: " << lm_num_error[i] << ",\ttotal avg_error: " << lm_avg_error[i] << ",\ttotal max_error: " << lm_max_error[i] << std::endl;
			std::cout << "\tAvg. avg_error: " << std::to_string(lm_avg_error[i]/(double)lm_num_error[i]);
			std::cout << ",\tAvg. max_error: " << std::to_string(lm_max_error[i]/(double)lm_num_error[i]) << std::endl;
		}
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if MODEL_BREAKDOWN
	for (int i=0; i<7; i++) {
		if (!(lm_num[i])) continue;
		std::cout << "[ Level " << i << " ]\n";
		if (lm_num[i]) {
			std::cout << "\t# of segs in learned model - \tAVG: " << std::to_string(lm_segs[i]/(double)lm_num[i]) << ",\tnum: " << lm_num[i] << ",\ttotal: " << lm_segs[i] << "\n";
			std::cout << "\t# of keys in learned model - \tAVG: " << std::to_string(lm_keys[i]/(double)lm_num[i]) << ",\tnum: " << lm_num[i] << ",\ttotal: " << lm_keys[i] << "\n";
		}
	}
	std::cout << "----------------------------------------------------------" << std::endl;
#endif
#if TIME_W
	if (koo::num_learntime || koo::num_compactiontime2[0] || koo::num_compactiontime2[1] || koo::num_compactiontime2[2]) {
		/*std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "DoCompactionWork() function totally\n";
		for (int i=0; i<5; i++) {
			if (!(koo::num_compactiontime[i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			std::cout << "\tavg compaction time:\t" << std::to_string(koo::compactiontime[i]/((double)koo::num_compactiontime[i])) << " ns, num:\t" << koo::num_compactiontime[i] << ", Total compaction time:\t" << koo::compactiontime[i]/1000000 << " ms\n";
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "DoCompactionWork() function internal partly\n";
		for (int i=0; i<5; i++) {
			if (!(koo::num_compactiontime2[i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			std::cout << "\tavg compaction time:\t" << std::to_string(koo::compactiontime2[i]/((double)koo::num_compactiontime2[i])) << " ns, num:\t" << koo::num_compactiontime2[i] << ", Total compaction time:\t" << koo::compactiontime2[i]/1000000 << " ms\n";
		}*/
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Compaciton input/output files size\n";
		for (int i=0; i<5; i++) {
			if (!(koo::num_compactiontime2[i])) continue;
			std::cout << "[ Level " << i << "+" << i+1 << " ]\n";
			std::cout << "\tnum:\t" << koo::num_compactiontime2[i] << ", Total compaction time:\t" << koo::compactiontime2[i]/1000000 << " ms\n";
			std::cout << "\tTotal input size: \t" << koo::size_inputs[i] << " B, avg input size per compaction time: \t" << std::to_string(koo::size_inputs[i]*1000000/((double)koo::compactiontime2[i])) << " B/ms\n";
			std::cout << "\tTotal output size:\t" << koo::size_outputs[i] << " B, avg output size per compaction time:\t" << std::to_string(koo::size_outputs[i]*1000000/((double)koo::compactiontime2[i])) << " B/ms\n";
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		if (koo::num_learntime) {
			std::cout << "Avg learning time per file = " << std::to_string((double)koo::learntime/(double)koo::num_learntime) << " ns,\t# learned files = " << koo::num_learntime << ",\tTotal time = " << std::to_string(koo::learntime) << std::endl;
			std::cout << "\nAvg # of items to learn = " << std::to_string((double)koo::learn_size/(double)koo::num_learn_size) << ",\tnum = " << koo::num_learn_size << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
	}
#endif
#if TIME_R
	if (koo::num_i_path || koo::num_m_path) {
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Index block path avg time = " << std::to_string(koo::i_path/((double)koo::num_i_path)) << " ns,\t# index block path = " << koo::num_i_path << ",\tTotal time = " << std::to_string(koo::i_path) << std::endl;
		std::cout << "Learned model path avg time = " << std::to_string(koo::m_path/((double)koo::num_m_path)) << " ns,\t# model path = " << koo::num_m_path << ",\tTotal time = " << std::to_string(koo::m_path) << std::endl;
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
#if TIME_R_LEVEL
	if (koo::num_i_path_l[1] || koo::num_m_path_l[1]) {
		std::cout << "----------------------------------------------------------" << std::endl;
		uint64_t total_num_i_path = 0, total_num_m_path = 0;
		uint64_t total_i_path = 0, total_m_path = 0;
		for (unsigned l=0; l<7; l++) {
			if (koo::num_i_path_l[l] == 0 && koo::num_m_path_l[l] == 0) continue;
			std::cout << "[ Level " << l << " ]\n";
			std::cout << "\tIndex block path avg time = " << std::to_string(koo::i_path_l[l]/((double)koo::num_i_path_l[l])) << " ns,\t# index block path = " << koo::num_i_path_l[l] << ",\tTotal time = " << std::to_string(koo::i_path_l[l]) << std::endl;
			std::cout << "\tModel path avg time = " << std::to_string(koo::m_path_l[l]/((double)koo::num_m_path_l[l])) << " ns,\t# model path = " << koo::num_m_path_l[l] << ",\tTotal time = " << std::to_string(koo::m_path_l[l]) << std::endl;
			total_num_i_path += koo::num_i_path_l[l];
			total_num_m_path += koo::num_m_path_l[l];
			total_i_path += koo::i_path_l[l];
			total_m_path += koo::m_path_l[l];
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "Index block path avg time = " << std::to_string(total_i_path/((double)total_num_i_path)) << " ns,\t# index block path = " << total_num_i_path << ",\tTotal time = " << std::to_string(total_i_path) << std::endl;
		std::cout << "Model path avg time = " << std::to_string(total_m_path/((double)total_num_m_path)) << " ns,\t# merged model path = " << total_num_m_path << ",\tTotal time = " << std::to_string(total_m_path) << std::endl;
		std::cout << "----------------------------------------------------------" << std::endl;
		uint64_t total_num_path = total_num_i_path + total_num_m_path;
		uint64_t total_time = (total_i_path + total_m_path)/1000000000;
		std::cout << "Total # path: " << total_num_path << ", total time: " << total_time << " s\n";
		std::cout << "# IB path / # total path = " << std::to_string(total_num_i_path/(double)total_num_path) << ",\t# model path / # total path = " << std::to_string(total_num_m_path/(double)total_num_path) << std::endl;
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
		int64_t total_U_time[config::kNumLevels] = {0, };
		uint32_t num_U_time[config::kNumLevels] = {0, };
		//std::ofstream ofs("/koo/tests/hyperbourbon-always/03.11_3/sst_lifespan/l_t1-1.txt");
		std::string str = "";
		for (int i=0; i<config::kNumLevels; i++) {
			if (lss[i].empty()) continue;
			//ofs << "Level " << i << " num: " << lss[i].size() << std::endl;
			str += "Level " + to_string(i) + " num: " + to_string(lss[i].size()) + "\n";
			for (auto& ls : lss[i]) {
				//ofs << ls.first << " " << ls.second.T_start << " " << ls.second.T_end << " " << ls.second.W_end << " " << ls.second.M_end << " " << ls.second.U_end << std::endl;
				std::string learned_str = "";
				if (ls.second.learned) learned_str = "1";
				else learned_str = "0";
				str += to_string(ls.first) + " " + learned_str + " " + to_string(ls.second.T_start) + " ";
				str += to_string(ls.second.T_end) + " " + to_string(ls.second.W_end) + " ";
				str += to_string(ls.second.M_end) + " " + to_string(ls.second.U_end) + "\n";

				if (ls.second.T_end != 0) {
					total_T_time[i] += ls.second.T_end - ls.second.T_start;
					num_T_time[i]++;

					if (ls.second.W_end != 0) {
						total_W_time[i] += (int64_t)(ls.second.W_end - ls.second.T_end);
						num_W_time[i]++;
						if ((int64_t)(ls.second.W_end - ls.second.T_end) > 0) {
							total_W_time_pos[i] += (int64_t)(ls.second.W_end - ls.second.T_end);
							num_W_time_pos[i]++;
						}

						if (ls.second.M_end != 0) {		// not L0 files ?이게 뭔말이야
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
			std::cout << "\t\tAvg. positive W_time: " << std::to_string(total_W_time_pos[i]/(double)num_W_time_pos[i]) << ",\t\tTotal: " << total_W_time_pos[i] << ",\tnum: " << num_W_time_pos[i] << std::endl;
			std::cout << "\tAvg. M_time: " << std::to_string(total_M_time[i]/(double)num_M_time[i]) << ",\t\tTotal: " << total_M_time[i] << ",\tnum: " << num_M_time[i] << std::endl;
			std::cout << "\tAvg. U_time: " << std::to_string(total_U_time[i]/(double)num_U_time[i]) << ",\t\tTotal: " << total_U_time[i] << ",\tnum: " << num_U_time[i] << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "학습한 테이블 정보만 모은 total time\n";
		int64_t total_T_time_level = 0;
		int64_t total_W_time_level = 0;
		int64_t total_M_time_level = 0;
		int64_t total_U_time_level = 0;
		uint32_t num_time = 0, num_U_end0 = 0, num_notlearned = 0;
		for (int l=0; l<config::kNumLevels; l++) {
			if (lss[l].empty()) continue;
			for (auto& ls : lss[l]) {
				//if (!ls.second.M_end) continue;
				if (!ls.second.T_end) continue;
				if (!ls.second.learned) { num_notlearned++; continue; }
				if (!ls.second.U_end) { num_U_end0++; continue; }
				total_T_time_level += ls.second.T_end - ls.second.T_start;
				total_W_time_level += (int64_t)(ls.second.W_end - ls.second.T_end);
				total_M_time_level += (int64_t)(ls.second.M_end - ls.second.W_end);
				total_U_time_level += (int64_t)(ls.second.U_end - ls.second.M_end);
				num_time++;
			}
		}
		if (num_time) {
			std::cout << "\t# learned tables: " << num_time << ", # not learned tables: " << num_notlearned << ", # tables learned when workload is ending: " << num_U_end0 << std::endl;
			std::cout << "\tAvg. T_time: " << std::to_string(total_T_time_level/(double)num_time) << ",\tTotal T_time: " << total_T_time_level << std::endl;
			std::cout << "\tAvg. W_time: " << std::to_string(total_W_time_level/(double)num_time) << ",\tTotal W_time: " << total_W_time_level << std::endl;
			std::cout << "\tAvg. M_time: " << std::to_string(total_M_time_level/(double)num_time) << ",\tTotal M_time: " << total_M_time_level << std::endl;
			std::cout << "\tAvg. U_time: " << std::to_string(total_U_time_level/(double)num_time) << ",\tTotal U_time: " << total_U_time_level << std::endl;
		}
		std::cout << "----------------------------------------------------------" << std::endl;
		std::cout << "학습한 테이블 정보만 모은 레벨별 time\n";
		for (int l=0; l<config::kNumLevels; l++) {
			if (lss[l].empty()) continue;
			total_T_time_level = 0;
			total_W_time_level = 0;
			total_M_time_level = 0;
			total_U_time_level = 0;
			num_time = 0;
			num_U_end0 = 0;
			num_notlearned = 0;
			for (auto& ls : lss[l]) {
				//if (!ls.second.M_end) continue;
				if (!ls.second.T_end) continue;
				if (!ls.second.learned) { num_notlearned++; continue; }
				if (!ls.second.U_end) { num_U_end0++; continue; }
				total_T_time_level += ls.second.T_end - ls.second.T_start;
				total_W_time_level += (int64_t)(ls.second.W_end - ls.second.T_end);
				total_M_time_level += (int64_t)(ls.second.M_end - ls.second.W_end);
				total_U_time_level += (int64_t)(ls.second.U_end - ls.second.M_end);
				num_time++;
			}
			if (num_time) {
				std::cout << "[ Level " << l << " Tables ] num: " << lss[l].size() << std::endl;
				std::cout << "\t# learned tables: " << num_time << ", # not learned tables: " << num_notlearned << ", # tables learned when workload is ending: " << num_U_end0 << std::endl;
				std::cout << "\tAvg. T_time: " << std::to_string(total_T_time_level/(double)num_time) << ",\tTotal T_time: " << total_T_time_level << std::endl;
				std::cout << "\tAvg. W_time: " << std::to_string(total_W_time_level/(double)num_time) << ",\tTotal W_time: " << total_W_time_level << std::endl;
				std::cout << "\tAvg. M_time: " << std::to_string(total_M_time_level/(double)num_time) << ",\tTotal M_time: " << total_M_time_level << std::endl;
				std::cout << "\tAvg. U_time: " << std::to_string(total_U_time_level/(double)num_time) << ",\tTotal U_time: " << total_U_time_level << std::endl;
			}
		}
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
#endif
#if TIME_W
	learntime = 0;
	num_learntime = 0;
		for (int j=0; j<5; j++) {
			compactiontime[j] = 0;
			num_compactiontime[j] = 0;
			compactiontime2[j] = 0;
			num_compactiontime2[j] = 0;
			size_inputs[j] = 0;
			num_inputs[j] = 0;
			size_outputs[j] = 0;
			num_outputs[j] = 0;
		}
	learn_size = 0;
	num_learn_size = 0;
#endif
#if TIME_R_LEVEL
	for (int i=0; i<7; i++) {
		num_i_path_l[i] = 0;
		num_m_path_l[i] = 0;
		i_path_l[i] = 0;
		m_path_l[i] = 0;
	}
#endif
}
}
