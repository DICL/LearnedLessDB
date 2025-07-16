#pragma once
#include <iostream>
#include <chrono>
#include <math.h>
#include <fstream>
#include <atomic>
#include <thread>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <experimental/filesystem>
#include <map>
#include <ctime>
#include <cmath>
#include <mutex>
#include <condition_variable>

#define YCSB_CXX 1
#define YCSB_WRAPPER 1		// produce requests before run workloads + skip writing latency files
#define YCSB_THROUGHPUTHIST 0	// record throughput history (no latency)
#define YCSB_LOAD_THREAD 0
#define YCSB_KEY 0				// 키 생성 미리 안하고 operation 동작 직전에 키 생성 (Load만)
#define YCSB_DB 0		// load db 똑같이 유지 (no read trigger compaction, load 끝나고 db 닫고 다시 열고 남은 compaction 다 하고 닫고 다시 열어서 워크로드) -> 실패
#define YCSB_COPYDB 1			// SpanDB ycsb load 외 워크로드 전 db copy -> copy한 db에서 워크로드 실험
#define OFFLINE_FILELEARN 0		// delete db시 남아있는 모든 sst 학습
#define YCSB_SOSD 0		// records=600M

#define VLOG 1
#define THREADSAFE 1
#define USE_BUFFER 0
#define MULTI_COMPACTION 1
#define REMOVE_MUTEX 1			// shared_ptr로 Get 위한 current mem, imm, version 관리 // REMOVE_MUTEX2->REMOVE_MUTEX
#define REMOVE_MUTEX2 1			// SequenceWriteBegin mutex
#define REMOVE_READTRIGGERCOMP 0

#define LEARN 1
#define BOPTION 0				// Bourbon default file, level size option
#define LEARN_MODEL_ERROR 8		// (default) --> learn_model_error로 변경
#define MERGE_MODEL_ERROR 21		// (default, 최대 52까지 가능) --> merge_model_error로 변경 (retraining threshold)
//#define IDLE_LEARNING 1		// when learning thread is idle, learn tables with merged models (replace merged models into learned models)
#define NORMARLIZE_KEY 0		// key - min_key. 선분 k, b만

#define MERGE 1
#define SKIP_SEG 30			// default. merge_new.cc
//#define SKIP_SEG 10
#define MAX_MERGE_HISTORY 0				// 0: off, 1~: merge_history limit (Merge Footprint)
//#define MAX_MERGE_HISTORY 20
#define OPT1 1									// Compaction 내 Compare() 횟수 1/n으로 줄여보자	TODO
#define OPT2 0								// [max(pos-e, min_y), min(pos+e, max_y)]	TODO 성능 실험
#define OPT4 0								// compaction 전 lq_len < OPT4면 model merging 안하고 learning

#define MODEL_COMPACTION 1		// compaction using merged models

#define RETRAIN 1
#define RETRAIN2 1						// 재학습 대신 error bound 확장
#define RETRAIN3 1						// 재학습하는 파일은 우선순위 낮게

#define L0_MAP 0		// level0_map으로 l0 table 학습
#define LEVEL0_FILE_LEARN 1000
#define LEARN_L0 1			// 항상 l0 table 학습 (after flush)
#define NO_STALL 0			// L0 write stall 없애고 L0 bloom filter 키우기

#define EXTENT_HASH 0		// YCSB_CXX on
//#define BUCKET_LEN 5001
//#define BUCKET_LEN 6007
//#define BUCKET_LEN 7001			// default
#define BUCKET_LEN 8501			// 37,7001이 더 빠르지만 throughput은 37,8501가 더 높음(BI가 더 적어서. 다시)
//#define BUCKET_LEN 9001
//#define EH_N 40
//#define EH_N 38
#define EH_N 37						// default
#define EH_MAX_NUM_SEG 5000		// 7001 40
//#define EH_MAX_NUM_SEG 2600		// 6007 39
//#define EH_MAX_NUM_SEG 3000			// default
#define EXTENT_HASH_DEBUG 0		// /koo/Extent-hashing/linears/pm/data_ycsb/


#define SST_LIFESPAN 0	// level별 SST의 lifespan. table 생성(T) -> model 생성(M) -> model 사용(U)
// TODO workload 끝에 학습 취소되는 테이블들 계산에서 빼야함

#define LEARNING_IO 0

#define MIXGRAPH 0
#define DEBUG 0
#define AC_TEST 0
#define AC_TEST_HISTORY 0
#define AC_TEST2 0		// retrainig threshold test
#define AC_TEST3 0
#define TIME_W 0
#define TIME_W_DETAIL 0
#define TIME_R 0
#define TIME_R_DETAIL 0
#define TIME_R_LEVEL 0
#define MULTI_COMPACTION_CNT 0
#define MC_DEBUG 0
#define EH_AC_TEST 0
#define EH_TIME_R 0
#define TIME_MODELCOMP 0		// dbformat.h 주의!
#define MODELCOMP_TEST 0
#define MODEL_BREAKDOWN 0
#define MODEL_ACCURACY 0			// WriteModel시 TestModelAccuracy()
#define LOOKUP_ACCURACY 0

namespace koo {

extern std::string model_dbname;
extern double learn_model_error;
extern double merge_model_error;

#if LOOKUP_ACCURACY
extern std::atomic<uint64_t> lm_error[7];
extern std::atomic<uint64_t> lm_num_error[7];
extern std::atomic<uint64_t> mm_error[7];
extern std::atomic<uint64_t> mm_num_error[7];
extern std::atomic<uint64_t> rm_error[7];
extern std::atomic<uint64_t> rm_num_error[7];
#endif

#if MODEL_ACCURACY
extern std::atomic<uint64_t> lm_max_error[7];
extern std::atomic<uint64_t> lm_avg_error[7];
extern std::atomic<uint64_t> lm_num_error2[7];
extern std::atomic<uint64_t> mm_max_error[7];
extern std::atomic<uint64_t> mm_avg_error[7];
extern std::atomic<uint64_t> mm_num_error2[7];

extern std::atomic<uint64_t> mm_max_error_over[7];
extern std::atomic<uint64_t> mm_avg_error_over[7];
extern std::atomic<uint64_t> mm_num_error_over[7];
extern std::atomic<uint64_t> mm_cnt_overmax[7];
#endif

#if MODEL_BREAKDOWN
extern std::atomic<uint64_t> lm_segs[7];	// learned model # of segs
extern std::atomic<uint64_t> lm_keys[7];	// learned model # of keys
extern std::atomic<uint64_t> lm_num[7];
extern std::atomic<uint64_t> mm_segs[7];	// merged model
extern std::atomic<uint64_t> mm_keys[7];	// merged model
extern std::atomic<uint64_t> mm_num[7];

extern uint64_t num_lm[7];		// # of learned models
extern uint64_t num_mm[7];		// # of merged models
#endif

#if TIME_MODELCOMP
extern uint64_t sum_micros;
extern uint64_t sum_waitimm;
#endif

#if YCSB_LOAD_THREAD
extern bool only_load;
#endif

#if THREADSAFE
class SpinLock {
 public:
  SpinLock() : flag_(false){}
  void lock() {
  	bool expect = false;
  	while (!flag_.compare_exchange_weak(expect, true)){
  		expect = false;
		}
	}
	void unlock(){
		flag_.store(false);
	}

 private:
  std::atomic<bool> flag_;
};

class RWLock {
 public:
	void LockWrite() {
		while (writer_active.exchange(true, std::memory_order_acquire)) {}
		while (reader_count.load(std::memory_order_acquire) > 0) {
			std::this_thread::yield();
		}
	}
	void UnlockWrite() {
		writer_active.store(false, std::memory_order_release);
	}
	void LockRead() {
		while (true) {
			while (writer_active.load(std::memory_order_acquire)) {
				std::this_thread::yield();
			}
			reader_count.fetch_add(1, std::memory_order_acquire);
			if (!writer_active.load(std::memory_order_acquire)) break;
			reader_count.fetch_sub(1, std::memory_order_release);
		}
	}
	void UnlockRead() {
		reader_count.fetch_sub(1, std::memory_order_release);
	}

 private:
	std::atomic<int> reader_count{0};
	std::atomic<bool> writer_active{false};
};
#endif

#if OPT4
extern uint32_t lq_len;
#endif

#if SST_LIFESPAN
// TODO start time을 hashmap에 기록? FileMetaData에 기록?
struct FileLifespanData {
	bool learned;
	bool merged;
	uint32_t level;
	uint64_t T_start;		// table building started
	uint64_t T_end;			// ~ table building finished (LogAndApply)
	uint64_t W_end;			// ~ waiting for model building
	uint64_t M_end;			// ~ model building finished
	uint64_t U_end;			// ~ table deleted

	FileLifespanData() : learned(false), merged(false), level(0), T_start(0), T_end(0), W_end(0), M_end(0), U_end(0) {}

	FileLifespanData(uint32_t level_, uint64_t T_start_)
		: learned(false), merged(false), level(level_), T_start(T_start_), T_end(0), W_end(0), M_end(0), U_end(0) {}
};

struct hash_FileLifespanData {
	size_t operator()(const uint64_t &file_number) const {
		return file_number;
	}
};

extern std::mutex mutex_lifespan_;
extern std::unordered_map<uint64_t, FileLifespanData, hash_FileLifespanData> lifespans;		// <file_number, >
// TODO vector로?
#endif

#if AC_TEST
extern uint64_t num_files_flush;			// # of files generated by flush
extern uint64_t num_files_compaction;
extern uint64_t num_learned;
extern uint64_t num_merged;
extern uint64_t num_retrained;
extern std::atomic<uint32_t> num_tryretraining;
#if RETRAIN2
extern std::atomic<uint32_t> num_erroradded;
#endif
/*extern bool count_compaction_triggered_after_load;
extern uint64_t time_compaction_triggered_after_load;
extern uint32_t num_compaction_triggered_after_load;
extern uint32_t num_inputs_compaction_triggered_after_load;		// input files
extern uint32_t num_outputs_compaction_triggered_after_load;	// output files
extern int64_t size_inputs_compaction_triggered_after_load;				// input files
extern int64_t size_outputs_compaction_triggered_after_load;			// output files*/
#if TIME_W_DETAIL
extern uint64_t compactiontime_d[2][5];			// 0: w/o merging, 1: w/ merging
extern uint32_t num_compactiontime_d[2][5];
extern uint64_t bc_d[5];
extern uint32_t num_bc_d[5];

extern uint64_t time_filldata;
extern uint32_t num_filldata;
#endif

extern std::atomic<uint64_t> file_size[7];		// file size per level
extern std::atomic<uint64_t> num_files[7];
#endif

#if MC_DEBUG
/*extern uint32_t id_twait;
extern uint64_t time_twait[8];		// sleeping time per compaction thread
extern uint32_t num_twait[8];*/
extern std::atomic<uint64_t> time_tAppend;
extern std::atomic<uint32_t> num_tAppend;
#endif

#if AC_TEST2
struct alignas(64) PaddedAtomic {
	std::atomic<uint64_t> val;
};

extern PaddedAtomic served_i_time[6];
extern PaddedAtomic served_i[6];			// index block (L0-L5)
extern PaddedAtomic served_l_time[6];
extern PaddedAtomic served_l[6];			// learned model (L0-L5)
extern PaddedAtomic served_m_time[4];
extern PaddedAtomic served_m[4];			// merged model (L2-L5)
extern PaddedAtomic served_r_time[4];
extern PaddedAtomic served_r[4];			// retrained model (L2-L5)

extern std::atomic<uint32_t> merged_model_miss;
extern std::atomic<uint32_t> served_m_linear;			// merged model linear search
extern std::atomic<uint32_t> served_m_linear_fail;			// merged model linear search

extern std::atomic<uint64_t> linear_time;
extern std::atomic<uint32_t> linear_num;
#endif

#if AC_TEST3
extern std::atomic<uint32_t> num_files_compaction_[4];
extern std::atomic<uint32_t> num_files_learned_[4];
extern std::atomic<uint32_t> num_files_merged_[4];
#endif

#if TIME_W
extern uint64_t learntime;
extern uint32_t num_learntime;
extern uint64_t mergetime;
extern uint32_t num_mergetime;
//extern uint64_t compactiontime[2][5];
//extern uint32_t num_compactiontime[2][5];
extern uint64_t compactiontime2[2][7];
extern uint32_t num_compactiontime2[2][7];
extern uint64_t learntime_l0;
extern uint32_t num_learntime_l0;
extern uint64_t learn_bytesize;
extern uint32_t num_learn_bytesize;
extern uint64_t merge_bytesize;
extern uint32_t num_merge_bytesize;

extern uint64_t learn_size;
extern uint32_t num_learn_size;
extern uint64_t merge_size;
extern uint32_t num_merge_size;

extern int64_t size_inputs[2][7];
extern int64_t size_outputs[2][7];
extern uint32_t num_inputs[2][7];
extern uint32_t num_outputs[2][7];
#endif

#if TIME_R_DETAIL
//extern std::atomic<uint32_t> num_mem;
//extern std::atomic<uint32_t> num_imm;
extern std::atomic<uint32_t> num_ver;
//extern std::atomic<uint32_t> num_mem_succ;
//extern std::atomic<uint32_t> num_imm_succ;
//extern std::atomic<uint32_t> num_ver_succ;
//extern std::atomic<uint64_t> time_mem;
//extern std::atomic<uint64_t> time_imm;
extern std::atomic<uint64_t> time_ver;
extern std::atomic<uint32_t> num_vlog;
extern std::atomic<uint64_t> time_vlog;
#endif

#if TIME_R
extern std::atomic<uint32_t> num_i_path;			// # of index block path
extern std::atomic<uint32_t> num_l_path;			// # of learned model path
extern std::atomic<uint32_t> num_m_path;			// # of merged model path
extern std::atomic<uint64_t> i_path;			// total time of index block path
extern std::atomic<uint64_t> l_path;			// total time of learned model path
extern std::atomic<uint64_t> m_path;			// total time of merged model path
#endif
#if TIME_R_LEVEL
extern std::atomic<uint32_t> num_i_path_l[7];
extern std::atomic<uint32_t> num_l_path_l[7];
extern std::atomic<uint32_t> num_m_path_l[7];
extern std::atomic<uint64_t> i_path_l[7];
extern std::atomic<uint64_t> l_path_l[7];
extern std::atomic<uint64_t> m_path_l[7];
/*extern uint32_t num_i_path_l[7];
extern uint32_t num_l_path_l[7];
extern uint32_t num_m_path_l[7];
extern uint64_t i_path_l[7];
extern uint64_t l_path_l[7];
extern uint64_t m_path_l[7];*/
#endif

#if MULTI_COMPACTION_CNT
extern uint64_t num_PickCompactionLevel;
extern uint64_t num_PickCompaction;
extern uint64_t num_compactions;
extern uint64_t num_output_files;
#endif

#if EH_AC_TEST
extern uint64_t num_eh_insert;		// succ
extern uint64_t num_eh_insert_linearacc;
extern uint64_t num_eh_insert_fail;

extern uint64_t num_eh_get_total;
extern uint64_t num_eh_get;				// succ
extern uint64_t num_eh_get_linearacc;
extern uint64_t num_eh_get_fail;
#endif

#if EH_TIME_R
// level 별로 매우 다름
extern uint64_t eh_insert_time;
extern uint64_t eh_insert_num;
extern uint32_t eh_insert_full;
extern uint32_t eh_insert_toomanysegs;

extern std::atomic<uint64_t> eh_get_time;
extern std::atomic<uint64_t> eh_get_num;
extern std::atomic<uint64_t> bi_get_time;
extern std::atomic<uint64_t> bi_get_num;
#endif

#if MODELCOMP_TEST
extern std::atomic<uint64_t> num_inserts[2];
extern std::atomic<uint64_t> num_comparisons[2];
extern std::atomic<uint64_t> num_keys_lower[2];
extern std::atomic<uint64_t> num_keys_upper[2];
extern std::atomic<uint64_t> total_num_keys[2];

/*extern std::atomic<uint64_t> total_cpu_cycles[2];
extern std::atomic<uint32_t> num_cpu_cycles[2];
extern uint64_t rdtsc_start();
extern uint64_t rdtsc_end();*/
#endif

extern void Report();
extern void Reset();

}
