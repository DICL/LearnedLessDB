// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_MERGER_WITH_MODEL_H_
#define STORAGE_LEVELDB_TABLE_MERGER_WITH_MODEL_H_

#include <utility>
#include <vector>
#include <cstdint>
#include "koo/koo.h"

#if MODEL_COMPACTION
namespace leveldb {

class Compaction;
class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
extern Iterator* NewMergingWithModelIterator(
    const Comparator* comparator, Iterator** children, Compaction* c);

extern void PrintIterStats(Iterator* iter);
extern std::pair<void*, std::vector<uint64_t>*> ReturnMergeModel(bool include_current, Iterator* iter);
}  // namespace leveldb
#endif

#endif  // STORAGE_LEVELDB_TABLE_MERGER_H_
