//
// Created by daiyi on 2020/03/23.
// A very simple implementation of Wisckey's Value Log
// Since Bourbon doesn't test on deletion, Vlog garbage collection is not ported

#ifndef LEVELDB_VLOG_H
#define LEVELDB_VLOG_H

#include "hyperleveldb/env.h"
#include "port/port.h"
#include <atomic>
#include "koo/koo.h"

using namespace leveldb;

namespace koo {

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
#endif

class VLog {
private:
    WritableFile* writer;
    RandomAccessFile* reader;
    std::string buffer;
    uint64_t vlog_size;
#if THREADSAFE
		uint64_t vlog_flushed;
		port::Mutex mu_;
		SpinLock s_mu_;
#endif

    void Flush();

public:
    explicit VLog(const std::string& vlog_name);
    uint64_t AddRecord(const Slice& key, const Slice& value);
    std::string ReadRecord(uint64_t address, uint32_t size);
    void Sync();
    ~VLog();
};





}




#endif //LEVELDB_VLOG_H
