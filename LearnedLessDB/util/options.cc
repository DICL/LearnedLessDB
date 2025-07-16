// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "hyperleveldb/options.h"

#include "hyperleveldb/comparator.h"
#include "hyperleveldb/env.h"
#include "koo/koo.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
#if LEARN
      write_buffer_size(16<<20),
      //write_buffer_size(64<<20),
#else
      write_buffer_size(4<<20),
#endif
#if LEARN
      max_open_files(1024 * 1024),				// Bourbon의 adgMod::fd_limit, util/env_posix.cc MmapLimiter
      //max_open_files(1000),
#else
      max_open_files(1000),
#endif
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
#if LEARN
      compression(kNoCompression),		// Bourbon
      //compression(kSnappyCompression),
#else
      compression(kSnappyCompression),
#endif
#if LEARN
      //filter_policy(NewBloomFilterPolicy(10)),
      filter_policy(NULL),
#else
      filter_policy(NULL),
#endif
      manual_garbage_collection(false) {
}


}  // namespace leveldb
