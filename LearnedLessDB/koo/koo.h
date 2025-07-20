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

#define YCSB_COPYDB 1
#define YCSB_SOSD 0
#define NORMARLIZE_KEY 0		// key - min_key. 선분 k, b만

#define MODEL_COMPACTION 1		// compaction using merged models
#define RETRAIN 1
#define RETRAIN2 1						// 재학습 대신 error bound 확장
#define RETRAIN3 1						// 재학습하는 파일은 우선순위 낮게

namespace koo {

extern std::string model_dbname;
extern double learn_model_error;
extern double merge_model_error;
extern uint64_t min_num_keys;

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


}
