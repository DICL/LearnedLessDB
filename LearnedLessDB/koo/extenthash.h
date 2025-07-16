#pragma once

#include <cstdlib>
#include <bitset>
#include <vector>
#include <strings.h>
#include "koo/koo.h"

#define fls(x)				(64 - __builtin_clzll(x))
#define MASK(lcn)			((1 << fls(lcn)) - 1)


static inline uint64_t EH_HASH(uint64_t k) {
	return std::_Hash_bytes(&k, sizeof(uint64_t), 0xc70f6907UL) % BUCKET_LEN;
	//return std::_Fnv_hash_bytes(&k, sizeof(uint64_t), 0xc70f6907UL) % BUCKET_LEN;
	// return ((uint32_t)k) % BUCKET_LEN;
	//return (k * 1398 + 549) % BUCKET_LEN;
}

static inline uint64_t calc_stride_len(uint64_t lcn, uint32_t len) {
	if (len == 1 || lcn % 2) return 1;
	return 1U << (fls(std::min(len, 1U << (ffs(lcn) - 1))) - 1);
}


struct Extent {
	uint64_t lcn;			// the start key of the extent
	uint64_t len;			// the length of the extent

	Extent(uint64_t _lcn, uint64_t _len)
		: lcn(_lcn), len(_len) {}
};
