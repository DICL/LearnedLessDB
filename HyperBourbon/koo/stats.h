//
// Created by daiyi on 2019/09/30.
// A Singleton that contains timers to be easily used globally
// Though other globally used structures are directly set to be global variables instead...
// Usage: first param is the clock id to operate, second param is optional: 
// a flag if this time interval is recorded. 

#ifndef LEVELDB_STATS_H
#define LEVELDB_STATS_H


#include <cstdint>
#include <map>
#include <vector>
#include <cstring>
#include "timer.h"
#include "koo/koo.h"

using std::string;
using std::to_string;


namespace koo {

    class Timer;
    class Stats {
    private:
        static Stats* singleton;
        Stats();

#if THREADSAFE
        std::vector<Timer*> timers;
#else
        std::vector<Timer> timers;
#endif
    public:
        uint64_t initial_time;

        static Stats* GetInstance();
#if THREADSAFE
        uint64_t StartTimer(uint32_t id);
        std::pair<uint64_t, uint64_t> PauseTimer(uint64_t time_started, uint32_t id, bool record = false);
#else
        void StartTimer(uint32_t id);
        std::pair<uint64_t, uint64_t> PauseTimer(uint32_t id, bool record = false);
#endif
        void ResetTimer(uint32_t id);
        uint64_t ReportTime(uint32_t id);
        void ReportTime();

        uint64_t GetTime();
        void ResetAll();
        ~Stats();
    };


}


#endif //LEVELDB_STATS_H
