//
// Created by daiyi on 2020/02/02.
//

#include "timer.h"
#include "util.h"
#include <cassert>
#include <x86intrin.h>


namespace koo {

    Timer::Timer() : time_accumulated(0) {}

    uint64_t Timer::Start() {
        unsigned int dummy = 0;
        return __rdtscp(&dummy);
    }

    std::pair<uint64_t, uint64_t> Timer::Pause(uint64_t time_started, bool record) {
        unsigned int dummy = 0;
        uint64_t time_elapse = __rdtscp(&dummy) - time_started;
        time_accumulated += time_elapse / reference_frequency;

        if (record) {
            Stats* instance = Stats::GetInstance();
            uint64_t start_absolute = time_started - instance->initial_time;
            uint64_t end_absolute = start_absolute + time_elapse;
            return {start_absolute / reference_frequency, end_absolute / reference_frequency};
        } else {
            return {0, 0};
        }
    }

    void Timer::Reset() {
        time_accumulated = 0;
    }

    uint64_t Timer::Time() {
        //assert(!started);
        return time_accumulated;
    }
}
