#pragma once

#include <algorithm>
#include <vector>

namespace leveldb {

template <class T, size_t kSize = 8>
class autovector : public std::vector<T> {
	using std::vector<T>::vector;

 public:
	autovector() {
		std::vector<T>::reserve(kSize);
	}
};

} // leveldb
