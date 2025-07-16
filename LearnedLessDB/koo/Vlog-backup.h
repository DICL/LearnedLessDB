//
// Created by daiyi on 2020/03/23.
// A very simple implementation of Wisckey's Value Log
// Since Bourbon doesn't test on deletion, Vlog garbage collection is not ported

#ifndef LEVELDB_VLOG_H
#define LEVELDB_VLOG_H

#include "hyperleveldb/env.h"
#include "port/port.h"
#include <atomic>
#include <mutex>
#include "koo/koo.h"

using namespace leveldb;

namespace koo {

class VLog {
private:
    WritableFile* writer;
    RandomAccessFile* reader;
    std::string buffer;
    uint64_t vlog_size;
#if THREADSAFE
    uint64_t vlog_flushed;
    //port::Mutex mu_;
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
