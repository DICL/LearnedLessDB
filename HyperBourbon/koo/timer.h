//
// Created by daiyi on 2020/02/02.
// Internal implementation for timers used in Stats class.

#ifndef LEVELDB_TIMER_H
#define LEVELDB_TIMER_H


#include <cstdint>
#include <ctime>
#include <utility>
#include <vector>
#include <atomic>
#include "koo/koo.h"

namespace koo {

    class Timer {
#if THREADSAFE
        std::atomic<uint64_t> time_accumulated;
#else
        uint64_t time_started;
        uint64_t time_accumulated;
        bool started;
#endif

    public:
#if THREADSAFE
        uint64_t Start();
        std::pair<uint64_t, uint64_t> Pause(uint64_t time_started, bool record = false);
#else
        void Start();
        std::pair<uint64_t, uint64_t> Pause(bool record = false);
#endif
        void Reset();
        uint64_t Time();

        Timer();
        ~Timer() = default;
    };

}


#endif //LEVELDB_TIMER_H
