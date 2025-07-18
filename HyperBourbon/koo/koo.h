#pragma once

#include <iostream>
#include <chrono>
#include <math.h>
#include <fstream>
#include <atomic>
#include <cstdio>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <thread>
#include <unistd.h>
#include <sys/syscall.h>
#include <experimental/filesystem>
#include <mutex>
#include <condition_variable>

#define BOURBON_PLUS 1				// HyperBourbon(CBA)

#define LEARNING_ALL 0				// HyperBourbon(Always) (BOURBON_PLUS must be on)
#define BOURBON_OFFLINE 0			// HyperWiscKey (BOURBON_PLUS must be on)

#define YCSB_CXX 1
#define YCSB_WRAPPER 1		// produce requests before run workloads + skip writing latency files
#define YCSB_THROUGHPUTHIST 0	// record throughput history (no latency)
#define YCSB_LOAD_THREAD 0
//#define YCSB_LOAD_THREAD 16		// 다른 워크로드(A~F) 전 Load A의 client thread 개수 고정
#define YCSB_KEY 0							// 키 생성 미리 안하고 operation 동작 직전에 키 생성(Load만)
#define YCSB_MAKEKEYFILE 0
#define YCSB_DB 0		// load db 똑같이 유지 (no read trigger compaction, load 끝나고 db 닫고 다시 열고 남은 compaction 다 하고 닫고 다시 열어서 워크로드) -> 실패
#define YCSB_COPYDB 1		// SpanDB ycsb load 외 워크로드 전 db copy -> copy한 db에서 워크로드 실행
#define OFFLINE_FILELEARN 0		// delete db시 남아있는 모든 sst 학습
#define YCSB_SOSD 0

#define VLOG 1
#define THREADSAFE 1		// vlog, stats, timer
#define MULTI_COMPACTION 1
#define REMOVE_MUTEX 1			// shared_ptr로 Get 위한 current mem, imm, version 저장 REMOVE_MUTEX2->REMOVE_MUTEX
#define REMOVE_MUTEX2 1			// SequenceWriteBegin mutex
#define REMOVE_READTRIGGERCOMP 0

#define LEARN 1					// common LEARN
#define BLEARN 1				// LEARN for Bourbon

#define LEARN_MODEL_ERROR 8
#define LEARN_TRIGGER_TIME 50000000
//#define LEARN_TRIGGER_TIME 170000000
#define MULTI_LEARNING 1		// the number of learning threads (0: off)

#define MIXGRAPH 0
#define DEBUG 0
#define AC_TEST 0
#define AC_TEST_HISTORY 0
#define BREAKDOWN 0
#define TIME_W 0
#define TIME_W_DETAIL 0
#define TIME_R 0
#define TIME_R_DETAIL 0
#define TIME_R_LEVEL 0
#define MULTI_COMPACTION_CNT 0
#define MC_DEBUG 0
#define TIME_MODELCOMP 0		// dbformat.h 주의
#define SST_LIFESPAN 0	// level별 SST의 lifespan. table 생성(T) -> model 생성(M) -> model 사용(U)
												// TODO workload 끝에 학습 취소되는 테이블들 계산에서 빼야함
#define MODELCOMP_TEST 0
#define MODEL_BREAKDOWN 0
#define MODEL_ACCURACY 0
#define LOOKUP_ACCURACY 0

namespace koo {

extern std::string model_dbname;

#if LOOKUP_ACCURACY
extern std::atomic<uint64_t> lm_error[7];
extern std::atomic<uint64_t> lm_num_error[7];
#endif

#if MODEL_ACCURACY
extern std::atomic<uint64_t> lm_max_error[7];
extern std::atomic<uint64_t> lm_avg_error[7];
extern std::atomic<uint64_t> lm_num_error[7];
#endif

#if MODEL_BREAKDOWN
extern std::atomic<uint64_t> lm_segs[7];	// learned model # of segs
extern std::atomic<uint64_t> lm_keys[7];	// learned model # of keys
extern std::atomic<uint64_t> lm_num[7];

extern uint64_t num_lm[7];		// # of learned models
#endif

#if TIME_MODELCOMP
extern uint64_t sum_micros;
extern uint64_t sum_waittime;
//extern std::atomic<uint64_t> sum_read;
#endif

#if YCSB_LOAD_THREAD
extern bool only_load;
#endif

#if THREADSAFE
extern std::condition_variable cv;
extern std::mutex cv_mtx;
extern std::atomic<bool> should_stop;

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

#if SST_LIFESPAN
// TODO start time을 hashmap에 기록? FileMetaData에 기록?
struct FileLifespanData {
	bool learned;
	uint32_t level;
	uint64_t T_start;		// table building started
	uint64_t T_end;			// ~ table building finished (LogAndApply)
	uint64_t W_end;			// ~ waiting for model learning
	uint64_t M_end;			// ~ model learning finished
	uint64_t U_end;			// ~ table deleted

	FileLifespanData() : learned(false), level(0), T_start(0), T_end(0), W_end(0), M_end(0), U_end(0) {}

	FileLifespanData(uint32_t level_, uint64_t T_start_)
		: learned(false), level(level_), T_start(T_start_), T_end(0), W_end(0), M_end(0), U_end(0) {}
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

extern bool count_compaction_triggered_after_load;
extern uint64_t time_compaction_triggered_after_load;
extern uint32_t num_compaction_triggered_after_load;
extern uint32_t num_inputs_compaction_triggered_after_load;			// input files
extern uint32_t num_outputs_compaction_triggered_after_load;		// output files
extern int64_t size_inputs_compaction_triggered_after_load;			// input files
extern int64_t size_outputs_compaction_triggered_after_load;		// output files
#if TIME_W_DETAIL
extern uint64_t compactiontime_d[5];
extern uint32_t num_compactiontime_d[5];
extern uint64_t bc_d[5];
extern uint32_t num_bc_d[5];

extern uint64_t time_filldata;
extern uint32_t num_filldata;
#endif
#if MC_DEBUG
/*extern uint32_t id_twait;
extern uint64_t time_twait[8];
extern uint32_t num_twait[8];*/
extern std::atomic<uint64_t> time_tAppend;
extern std::atomic<uint32_t> num_tAppend;
#endif
#endif

#if TIME_W
extern uint64_t learntime;
extern uint32_t num_learntime;
extern uint64_t compactiontime[5];
extern uint32_t num_compactiontime[5];
extern uint64_t compactiontime2[5];
extern uint32_t num_compactiontime2[5];
//extern uint64_t compactiontime;
//extern uint32_t num_compactiontime;

extern std::atomic<uint64_t> onlytrainingtime;
extern std::atomic<uint64_t> num_onlytrainingtime;
extern std::atomic<uint64_t> learn_bytesize;

extern uint64_t learn_size;
extern uint32_t num_learn_size;

extern int64_t size_inputs[5];
extern int64_t size_outputs[5];
extern uint32_t num_inputs[5];
extern uint32_t num_outputs[5];
#endif

#if TIME_R
extern std::atomic<uint32_t> num_i_path;			// # of index block path
extern std::atomic<uint32_t> num_m_path;			// # of model path
extern std::atomic<uint64_t> i_path;			// total time of index block path
extern std::atomic<uint64_t> m_path;			// total time of model path
#endif
#if TIME_R_DETAIL
extern std::atomic<uint32_t> num_ver;
extern std::atomic<uint64_t> time_ver;
extern std::atomic<uint32_t> num_vlog;
extern std::atomic<uint64_t> time_vlog;
#endif
#if TIME_R_LEVEL
extern std::atomic<uint32_t> num_i_path_l[7];
extern std::atomic<uint32_t> num_m_path_l[7];
extern std::atomic<uint64_t> i_path_l[7];
extern std::atomic<uint64_t> m_path_l[7];
#endif

#if MULTI_COMPACTION_CNT
extern uint64_t num_PickCompactionLevel;
extern uint64_t num_PickCompaction;
extern uint64_t num_compactions;
extern uint64_t num_output_files;
#endif

#if MODELCOMP_TEST 
/*extern std::atomic<uint64_t> num_comparisons[2];
//extern std::atomic<uint64_t> num_keys_lower[2];
//extern std::atomic<uint64_t> num_keys_upper[2];
extern std::atomic<uint64_t> total_num_keys[2];*/

extern std::atomic<uint64_t> total_cpu_cycles[2];
extern std::atomic<uint32_t> num_cpu_cycles[2];
extern uint64_t rdtsc_start();
extern uint64_t rdtsc_end();
#endif

extern void Report();
extern void Reset();

}
