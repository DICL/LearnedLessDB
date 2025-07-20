//
// Created by daiyi on 2020/02/12.
// Levelled counter that can record some integers for each level

#ifndef PROJECT1_COUNTER_H
#define PROJECT1_COUNTER_H

#include "../db/dbformat.h"
#include <vector>
#include "koo/koo.h"


class Counter {
    friend class CBModel_Learn;
private:
	std::atomic<uint64_t> count0;
	std::atomic<uint64_t> count1;
	std::atomic<uint64_t> count2;
	std::atomic<uint64_t> count3;
	std::atomic<uint64_t> count4;
	std::atomic<uint64_t> count5;
	std::atomic<uint64_t> count6;
	std::atomic<uint64_t> num0;
	std::atomic<uint64_t> num1;
	std::atomic<uint64_t> num2;
	std::atomic<uint64_t> num3;
	std::atomic<uint64_t> num4;
	std::atomic<uint64_t> num5;
	std::atomic<uint64_t> num6;
public:
    std::string name;

	Counter() : 
		count0(0), count1(0), count2(0), count3(0), count4(0), count5(0), count6(0),
		num0(0), num1(0), num2(0), num3(0), num4(0), num5(0), num6(0) {};
    void Increment(int level, uint64_t n = 1);
    void Reset();

};


#endif //PROJECT1_COUNTER_H
