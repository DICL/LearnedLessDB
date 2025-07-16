// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "hyperleveldb/cache.h"
#include "hyperleveldb/db.h"
#include "hyperleveldb/env.h"
#include "hyperleveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "util/rate_limiter.h"
#include <condition_variable>
#include <thread>
#include <string_view>
#include "koo/koo.h"

#if MIXGRAPH
#include "util/histogram_listdb.h"
#else
#include "util/histogram.h"
#endif

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,"
    "mixgraph,"
    ;

// Number of key/values to place in database
static int FLAGS_num = 1000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
//static int FLAGS_value_size = 100;
static int FLAGS_value_size = 8;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
//static double FLAGS_compression_ratio = 0.5;
static double FLAGS_compression_ratio = 1.0;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
//static int FLAGS_cache_size = -1;
static int FLAGS_cache_size = 8*1024*1024;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// Use the db with the following name.
//static const char* FLAGS_db = NULL;
static const char* FLAGS_db = "/mnt-koo/db/";

#if MIXGRAPH
constexpr size_t kStringKeyLength = 16;
static int32_t FLAGS_key_size = 16;
static int FLAGS_writes = -1;
static int64_t FLAGS_seed = 0;
static int32_t FLAGS_duration = 0;
static int32_t FLAGS_ops_between_duration_checks = 1000;

static double FLAGS_write_rate = 1000000.0;
static double FLAGS_read_rate = 1000000.0;
static double FLAGS_sine_c = 0;
/*static double FLAGS_sine_a = 1;
static double FLAGS_sine_b = 1;
static double FLAGS_sine_d = 1;*/
static double FLAGS_sine_a = 200*1000;		// light
static double FLAGS_sine_b = 0.03;
static double FLAGS_sine_d = 900*1000;
/*static double FLAGS_sine_a = 200*1000;		// heavy
static double FLAGS_sine_b = 0.03;
static double FLAGS_sine_d = 600*1000;*/

static double FLAGS_mix_get_ratio = 0.83;
static double FLAGS_mix_put_ratio = 0.17;
static double FLAGS_mix_seek_ratio = 0.0;
//static bool FLAGS_sine_mix_rate = false;		// rocksdb default
static bool FLAGS_sine_mix_rate = true;
static double FLAGS_sine_mix_rate_noise = 0.0;		// rocksdb default
//static double FLAGS_sine_mix_rate_noise = 0.5;		// listdb
static int64_t FLAGS_mix_max_scan_len = 10000;
static int64_t FLAGS_mix_max_value_size = 1024;
//static int64_t FLAGS_mix_ave_kv_size = 512;
//static uint64_t FLAGS_sine_mix_rate_interval_milliseconds = 10000;		// rocksdb default
static uint64_t FLAGS_sine_mix_rate_interval_milliseconds = 5000;
static int64_t FLAGS_report_interval_seconds = 0;
static const char* FLAGS_report_file = "mixgraph/report.csv";
static int64_t FLAGS_stats_interval = 0;
static int64_t FLAGS_stats_interval_seconds = 0;

const uint64_t kMicrosInSecond = 1000 * 1000;
#endif

namespace leveldb {

namespace {

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;
#if MIXGRAPH
	char w_value_buf[4096];
#endif

 public:
  RandomGenerator() : data_(), pos_() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
#if MIXGRAPH
		for (int i=0; i<4096; i++) w_value_buf[i] = (char)i;
#endif
  }

  Slice Generate(size_t len) {
#if MIXGRAPH
		std::string r_value;
		return Slice(w_value_buf, len);
#else
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
#endif
  }
};

static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

#if MIXGRAPH
static std::string TimeToString(uint64_t secondsSince1970) {
	const time_t seconds = (time_t)secondsSince1970;
	struct tm t;
	int maxsize = 64;
	std::string dummy;
	dummy.reserve(maxsize);
	dummy.resize(maxsize);
	char* p = &dummy[0];
	localtime_r(&seconds, &t);
	snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
				   t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
	return dummy;
}

// a class that reports stats to CSV file
class ReporterAgent {
 public:
  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs)
      : env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_);
    if (s.ok()) {
      s = report_file_->Append(Header() + "\n");
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }

    //reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
    reporting_thread_ = std::thread([&]() { SleepAndReport(); });
  }

  ~ReporterAgent() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
  }

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

 private:
  std::string Header() const { return "secs_elapsed,interval_qps"; }
  void SleepAndReport() {
    auto time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      //std::string report = ToString(secs_elapsed) + "," +
      //                     ToString(total_ops_done_snapshot - last_report_) +
      std::string report = std::to_string(secs_elapsed) + "," +
                           std::to_string(total_ops_done_snapshot - last_report_) +
                           "\n";
      auto s = report_file_->Append(report);
      if (s.ok()) {
        s = report_file_->Flush();
      }
      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }

  Env* env_;
  //std::unique_ptr<WritableFile> report_file_;
  WritableFile* report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  //leveldb::port::Thread reporting_thread_;
  std::thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};
#endif

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
                          OperationTypeString = {
  {kRead, "read"},
  {kWrite, "write"},
  {kDelete, "delete"},
  {kSeek, "seek"},
  {kMerge, "merge"},
  {kUpdate, "update"},
  {kCompress, "compress"},
  {kCompress, "uncompress"},
  {kCrc, "crc"},
  {kHash, "hash"},
  {kOthers, "op"}
};

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
#if MIXGRAPH
	int id_;
	uint64_t last_report_done_;
	uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
										 std::hash<unsigned char>> hist_;
#else
  Histogram hist_;
#endif
  std::string message_;
  uint64_t sine_interval_;
#if MIXGRAPH
  ReporterAgent* reporter_agent_ = nullptr;		// does not own
#endif

 public:
#if MIXGRAPH
  Stats(int id) 
    : start_(0),
#else
  Stats() 
    : start_(),
#endif
      finish_(),
      seconds_(),
      done_(),
      next_report_(),
      bytes_(),
      last_op_finish_(),
      //hist_(),
      message_() {
#if MIXGRAPH
    Start(id);
#else
    Start();
#endif
  }

#if MIXGRAPH
  void SetReporterAgent(ReporterAgent* reporter_agent) {
  	reporter_agent_ = reporter_agent;
	}
#endif

#if MIXGRAPH
  void Start(int id) {
  	id_ = id;
		next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
		last_report_done_ = 0;
    hist_.clear();
    last_op_finish_ = start_;
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Env::Default()->NowMicros();
    sine_interval_ = Env::Default()->NowMicros();
    finish_ = start_;
		last_report_finish_ = start_;
    message_.clear();
  }
#else
  void Start() {
    next_report_ = 100;
    hist_.Clear();
    last_op_finish_ = start_;
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Env::Default()->NowMicros();
    finish_ = start_;
    message_.clear();
    sine_interval_ = Env::Default()->NowMicros();
  }
#endif

  void Merge(const Stats& other) {
#if MIXGRAPH
    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
    	auto this_it = hist_.find(it->first);
    	if (this_it != hist_.end()) {
    		this_it->second->Merge(*(other.hist_.at(it->first)));
			} else {
				hist_.insert({it->first, it->second});
			}
		}
#else
    hist_.Merge(other.hist_);
#endif

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Env::Default()->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void ResetSineInterval() {
  	sine_interval_ = Env::Default()->NowMicros();
	}

	uint64_t GetSineInterval() {
		return sine_interval_;
	}

	uint64_t GetStart() {
		return start_;
	}

#if MIXGRAPH
	void FinishedOps(DB* db, int64_t num_ops, enum OperationType op_type = kOthers) {
		if (reporter_agent_) {
			reporter_agent_->ReportFinishedOps(num_ops);
		}

    if (FLAGS_histogram) {
      double now = Env::Default()->NowMicros();
      double micros = now - last_op_finish_;
      if (hist_.find(op_type) == hist_.end()) {
      	auto hist_temp = std::make_shared<HistogramImpl>();
      	hist_.insert({op_type, std::move(hist_temp)});
			}
			hist_[op_type]->Add(micros);

      /*if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }*/
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
    	if (!FLAGS_stats_interval) {
	      if      (next_report_ < 1000)   next_report_ += 100;
		    else if (next_report_ < 5000)   next_report_ += 500;
			  else if (next_report_ < 10000)  next_report_ += 1000;
				else if (next_report_ < 50000)  next_report_ += 5000;
	      else if (next_report_ < 100000) next_report_ += 10000;
		    else if (next_report_ < 500000) next_report_ += 50000;
			  else                            next_report_ += 100000;
				fprintf(stderr, "... finished %d ops%30s\r", done_, "");
	    } else {
	      uint64_t now = Env::Default()->NowMicros();
		    int64_t usecs_since_last = now - last_report_finish_;
			  if (FLAGS_stats_interval_seconds &&
						usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
					next_report_ += FLAGS_stats_interval;
				} else {
					fprintf(stderr,
                  "%s ... thread %d: (%lu,%d) ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  TimeToString(now / 1000000).c_str(), id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);
          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
				}
			}
	    fflush(stderr);
    }
	}
#else
  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = Env::Default()->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      fflush(stderr);
    }
  }
#endif

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

#if MIXGRAPH
		double elapsed = (finish_ - start_) * 1e-6;
		double throughput = (double)done_/elapsed;
    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    fprintf(stdout, "\tstart_: %f, finish_: %f, done_: %d, seconds_: %f\n",
						start_, finish_, done_, seconds_);

		if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
      	fprintf(stdout, "Microseconds per %s:\n%s\n",
								OperationTypeString[it->first].c_str(),
								it->second->ToString().c_str());
			}
		}
#else
    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
#endif
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
#if MIXGRAPH
  std::shared_ptr<RateLimiter> write_rate_limiter;
  std::shared_ptr<RateLimiter> read_rate_limiter;
#endif

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState()
    : mu(),
      cv(&mu),
      total(),
      num_initialized(),
      num_done(),
      start() {
  }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
#if MIXGRAPH
  Random64 rand;
#else
  Random rand;         // Has different seeds for different threads
#endif
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
#if MIXGRAPH
				rand((FLAGS_seed ? FLAGS_seed : 1000) + index),
        stats(index),
#else
        rand(1000 + index),
        stats(),
#endif
        shared() {
  }
 private:
  ThreadState(const ThreadState&);
  ThreadState& operator = (const ThreadState&);
};

#if MIXGRAPH
class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = Env::Default()->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = Env::Default()->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};
#endif

}  // namespace

class Benchmark {
 private:
  Benchmark(const Benchmark&);
  Benchmark& operator = (const Benchmark&);
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int writes_;
  int heap_counter_;
#if MIXGRAPH
	int key_size_;
	double read_random_exp_range_;
#endif

  void PrintHeader() {
    const int kKeySize = 16;
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %d\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
  : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
    filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
    db_(NULL),
    num_(FLAGS_num),
#if MIXGRAPH
		key_size_(FLAGS_key_size),
		read_random_exp_range_(0.0),
#endif
    value_size_(FLAGS_value_size),
    entries_per_batch_(1),
    write_options_(),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
#if MIXGRAPH
    writes_(FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes),
#endif
    heap_counter_(0) {
#if MIXGRAPH
    if (key_size_ > (int) kStringKeyLength) {
    	fprintf(stderr, "Error!: kStringKeyLength < --key_size\n.");
    	exit(1);
		}
#endif
    std::vector<std::string> files;
    Env::Default()->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        Env::Default()->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

#if MIXGRAPH
  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
  }

  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
    /*if (!keys_.empty()) {
      assert(FLAGS_use_existing_keys);
      assert(keys_.size() == static_cast<size_t>(num_keys));
      assert(v < static_cast<uint64_t>(num_keys));
      *key = keys_[v];
      return;
    }*/
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    /*if (keys_per_prefix_ > 0) {
      int64_t num_prefix = num_keys / keys_per_prefix_;
      int64_t prefix = v % num_prefix;
      int bytes_to_fill = std::min(prefix_size_, 8);
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
      }
      if (prefix_size_ > 8) {
        // fill the rest with 0s
        memset(pos + 8, '0', prefix_size_ - 8);
      }
      pos += prefix_size_;
    }*/

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
  }
#endif

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overriddden bwlow
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
#if MIXGRAPH
      writes_ = (FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes);
#endif
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("acquireload")) {
        method = &Benchmark::AcquireLoad;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
#if MIXGRAPH
      } else if (name == Slice("mixgraph")) {
      	method = &Benchmark::MixGraph;
#endif
      } else {
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, Options());
          Open();
        }
      }

      if (method != NULL) {
        RunBenchmark(num_threads, name, method);
      }
#if MIXGRAPH
			koo::Report();
			koo::Reset();
#endif
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

#if MIXGRAPH
    thread->stats.Start(thread->tid);
#else
    thread->stats.Start();
#endif
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

#if MIXGRAPH
    std::unique_ptr<ReporterAgent> reporter_agent;
    if (FLAGS_report_interval_seconds > 0) {
    	reporter_agent.reset(new ReporterAgent(Env::Default(), FLAGS_report_file,
																						 FLAGS_report_interval_seconds));
		}
#endif

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
#if MIXGRAPH
      arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
#endif
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kCrc);
#else
      thread->stats.FinishedSingleOp();
#endif
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    port::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kOthers);
#else
      thread->stats.FinishedSingleOp();
#endif
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kCompress);
#else
      thread->stats.FinishedSingleOp();
#endif
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kUncompress);
#else
      thread->stats.FinishedSingleOp();
#endif
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == NULL);
    Options options;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void WriteSeq(ThreadState* thread) {
    DoWrite(thread, true);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, false);
  }

#if VLOG && MIXGRAPH
  double SineRate(double x) {
    return FLAGS_sine_a*sin((FLAGS_sine_b*x) + FLAGS_sine_c) + FLAGS_sine_d;
  }

  int64_t GetRandomKey(Random64* rand) {
    uint64_t rand_int = rand->Next();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
      key_rand = (rand_int % (FLAGS_num - 1)) + 1;
    } else {
      const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
      long double order = -static_cast<long double>(rand_int % kBigInt) /
                          static_cast<long double>(kBigInt) *
                          read_random_exp_range_;
      long double exp_ran = std::exp(order);
      uint64_t rand_num =
          static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num));
      // Map to a different number to avoid locality.
      const uint64_t kBigPrime = 0x5bd1e995;
      // Overflow is like %(2^64). Will have little impact of results.
      key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num);
    }
    return key_rand;
  }
	enum WriteMode {
		RANDOM, SEQUENTIAL, UNIQUE_RANDOM
	};

  class KeyGenerator {
   public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {
      if (mode_ == UNIQUE_RANDOM) {
        // NOTE: if memory consumption of this approach becomes a concern,
        // we can either break it into pieces and only random shuffle a section
        // each time. Alternatively, use a bit map implementation
        // (https://reviews.facebook.net/differential/diff/54627/)
        values_.resize(num_);
        for (uint64_t i = 0; i < num_; ++i) {
          values_[i] = i;
        }
        RandomShuffle(values_.begin(), values_.end(),
                      static_cast<uint32_t>(FLAGS_seed));
      }
    }

    uint64_t Next() {
      switch (mode_) {
        case SEQUENTIAL:
          return next_++;
        case RANDOM:
          return (rand_->Next() % (num_ - 1)) + 1;
        case UNIQUE_RANDOM:
          assert(next_ < num_);
          return values_[next_++];
      }
      assert(false);
      return std::numeric_limits<uint64_t>::max();
    }

    // Only available for UNIQUE_RANDOM mode.
    uint64_t Fetch(uint64_t index) {
      assert(mode_ == UNIQUE_RANDOM);
      assert(index < values_.size());
      return values_[index];
    }

   private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
  };

  void DoWrite(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

		/*WriteMode write_mode = seq ? SEQUENTIAL : RANDOM;
  	const int64_t num_ops = writes_ == 0 ? num_ : writes_;
    //Duration duration(test_duration, max_ops, ops_per_stage);
		std::unique_ptr<KeyGenerator> key_gen;
		key_gen.reset(new KeyGenerator(&(thread->rand), write_mode, num_, num_ops));
		std::unique_ptr<const char[]> key_guard;
		Slice key = AllocateKey(&key_guard);*/

    RandomGenerator gen;
    Status s;
    int64_t bytes = 0;
    /*for (int i = 0; i < writes_; i++) {
    	int64_t rand_num = 0;
    	rand_num = key_gen->Next();
    	GenerateKeyFromInt(rand_num, FLAGS_num, &key);
    	//fprintf(stdout, "rand_num: %ld, key: %s\n\n", rand_num, key.data());
    	s = db_->Put(write_options_, key, gen.Generate(value_size_));
			if (!s.ok()) {
				fprintf(stderr, "put error: %s\n", s.ToString().c_str());
	      exit(1);
	    }
      thread->stats.FinishedOps(nullptr, 1, kWrite);
    	bytes += value_size_ + key_size_;
		}*/
    for (int i = 0; i < writes_; i += entries_per_batch_) {
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
				//if (strlen(key) != 16) fprintf(stdout, "strlen(key) != 16\t%s %lu\n", key, strlen(key));	// KOOO
				s = db_->Put(write_options_, key, gen.Generate(value_size_));
				if (!s.ok()) {
					fprintf(stderr, "put error: %s\n", s.ToString().c_str());
	        exit(1);
		    }
        bytes += value_size_ + strlen(key);
      }
      thread->stats.FinishedOps(nullptr, entries_per_batch_, kWrite);
    }
    thread->stats.AddBytes(bytes);
  }
#else
  void DoWrite(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
#if VLOG
        batch.Put(key, gen.Generate(value_size_));
#endif
        bytes += value_size_ + strlen(key);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }
#endif

#if MIXGRAPH
  // The inverse function of power distribution (y=ax^b)
  int64_t PowerCdfInversion(double u, double a, double b) {
    double ret;
    ret = std::pow((u / a), (1 / b));
    return static_cast<int64_t>(ceil(ret));
  }

  // Add the noice to the QPS
  double AddNoise(double origin, double noise_ratio) {
    if (noise_ratio < 0.0 || noise_ratio > 1.0) {
      return origin;
    }
    int band_int = static_cast<int>(FLAGS_sine_a);
    double delta = (rand() % band_int - band_int / 2) * noise_ratio;
    if (origin + delta < 0) {
      return origin;
    } else {
      return (origin + delta);
    }
  }

  // Decide the ratio of different query types
  // 0 Get, 1 Put, 2 Seek, 3 SeekForPrev, 4 Delete, 5 SingleDelete, 6 merge
  class QueryDecider {
   public:
    std::vector<int> type_;
    std::vector<double> ratio_;
    int range_;

    QueryDecider() {}
    ~QueryDecider() {}

    Status Initiate(std::vector<double> ratio_input) {
      int range_max = 1000;
      double sum = 0.0;
      for (auto& ratio : ratio_input) {
        sum += ratio;
      }
      range_ = 0;
      for (auto& ratio : ratio_input) {
        range_ += static_cast<int>(ceil(range_max * (ratio / sum)));
        type_.push_back(range_);
        ratio_.push_back(ratio / sum);
      }
      return Status::OK();
    }

    int GetType(int64_t rand_num) {
      if (rand_num < 0) {
        rand_num = rand_num * (-1);
      }
      assert(range_ != 0);
      int pos = static_cast<int>(rand_num % range_);
      for (int i = 0; i < static_cast<int>(type_.size()); i++) {
        if (pos < type_[i]) {
          return i;
        }
      }
      return 0;
    }
  };

  void MixGraph(ThreadState* thread) {
  	int64_t read = 0;
  	int64_t gets = 0;
  	int64_t puts = 0;
  	int64_t found = 0;
  	int64_t seek = 0;
  	int64_t seek_found = 0;
  	int64_t bytes = 0;
  	const int64_t default_value_max = 1 * 1024 * 1024;
  	int64_t value_max = default_value_max;
  	int64_t scan_len_max = FLAGS_mix_max_scan_len;
  	//double write_rate = 1000000.0;	//default
  	//double read_rate = 1000000.0;		//default
  	double write_rate = FLAGS_write_rate;		// sine_a, sine_d 단위에 맞추기
  	double read_rate = FLAGS_read_rate;
  	std::vector<double> ratio{FLAGS_mix_get_ratio, FLAGS_mix_put_ratio,
															FLAGS_mix_seek_ratio};
		//char value_buffer[default_value_max];
		QueryDecider query;
		RandomGenerator gen;
		Status s;
		if (value_max > FLAGS_mix_max_value_size) {
			value_max = FLAGS_mix_max_value_size;
		}

    ReadOptions options;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::string value;
    value.reserve(value_max);
    query.Initiate(ratio);

    // the limit of qps initiation
    //if (FLAGS_sine_a != 0 || FLAGS_sine_d != 0) {
      //thread->shared->read_rate_limiter.reset(NewGenericRateLimiter(
      //    static_cast<int64_t>(read_rate), 100000 /* refill_period_us */, 10 /* fairness */,
      //    RateLimiter::Mode::kReadsOnly));
    if (FLAGS_sine_mix_rate) {
      thread->shared->read_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(read_rate)));
      thread->shared->write_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(write_rate)));
		}

		//Duration duration(FLAGS_duration, reads_);
		//while (!duration.Done(1)) {
    for (int i = 0; i < reads_; i++) {
    	/*int64_t ini_rand = GetRandomKey(&thread->rand);
    	int64_t rand_v = ini_rand % FLAGS_num;
    	double u = static_cast<double>(rand_v) / FLAGS_num;
    	int64_t key_seed = PowerCdfInversion(u, 0.0, 0.0);
    	Random64 rand(key_seed);
    	int64_t key_rand = static_cast<int64_t>(rand.Next()) % FLAGS_num;
    	GenerateKeyFromInt(key_rand, FLAGS_num, &key);*/
			int64_t rand_v = thread->rand.Next() % FLAGS_num;
			const int k = thread->rand.Next() % FLAGS_num;
			char key[100];
			snprintf(key, sizeof(key), "%016d", k);
			int query_type = query.GetType(rand_v);

			// change the qps
			uint64_t now = Env::Default()->NowMicros();
			uint64_t usecs_since_last;
			if (now > thread->stats.GetSineInterval()) {
				usecs_since_last = now - thread->stats.GetSineInterval();
			} else {
				usecs_since_last = 0;
			}
			if (FLAGS_sine_mix_rate && usecs_since_last >
					(FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000})) {
        double usecs_since_start =
            static_cast<double>(now - thread->stats.GetStart());
        thread->stats.ResetSineInterval();
        double mix_rate_with_noise = AddNoise(
            SineRate(usecs_since_start / 1000000.0), FLAGS_sine_mix_rate_noise);
        read_rate = mix_rate_with_noise * (query.ratio_[0] + query.ratio_[2]);
        write_rate =
            mix_rate_with_noise * query.ratio_[1];
            //mix_rate_with_noise * query.ratio_[1] * FLAGS_mix_ave_kv_size;
        std::cout << read_rate << " " << write_rate << std::endl;
        std::cout << std::endl;

        //if (read_rate > 0) {
        if (read_rate >= 1) {
        	thread->shared->read_rate_limiter->SetBytesPerSecond(
							static_cast<int64_t>(read_rate));
				}
        //if (write_rate > 0) {
        if (write_rate >= 1) {
        	thread->shared->write_rate_limiter->SetBytesPerSecond(
							static_cast<int64_t>(write_rate));
				}
        /*thread->shared->write_rate_limiter.reset(
            NewGenericRateLimiter(static_cast<int64_t>(write_rate)));
        thread->shared->read_rate_limiter.reset(NewGenericRateLimiter(
            static_cast<int64_t>(read_rate),
            FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000}, 10,
            RateLimiter::Mode::kReadsOnly));*/
			}

			// Start the query
			if (query_type == 0) {
				gets++;
				read++;
				std::string value;
				s = db_->Get(options, key, &value);
				if (s.ok()) {		// Get
					found++;
					bytes += key_size_ + value_size_;
				} else if (!s.IsNotFound()) {
					fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
				} /*else {
					fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
				}*/

				if (thread->shared->read_rate_limiter && read % 100 == 0) {
					thread->shared->read_rate_limiter->Request(100, Env::IO_HIGH);
				}
        /*if (thread->shared->read_rate_limiter.get() != nullptr &&
            read % 256 == 255) {
          thread->shared->read_rate_limiter->Request(
              256, Env::IO_HIGH,
              RateLimiter::OpType::kRead);
        }*/
				thread->stats.FinishedOps(nullptr, 1, kRead);
			} else if (query_type == 1) {		// Put
				puts++;
				s = db_->Put(write_options_, key, gen.Generate(value_size_));
				if (!s.ok()) {
					fprintf(stderr, "put error: %s\n", s.ToString().c_str());
					exit(1);
				}

				if (thread->shared->write_rate_limiter && puts % 100 == 0) {
					thread->shared->write_rate_limiter->Request(100, Env::IO_HIGH);
				}
        /*if (thread->shared->write_rate_limiter) {
          thread->shared->write_rate_limiter->Request(
              strlen(key) + value_size_, Env::IO_HIGH,
              RateLimiter::OpType::kWrite);
        }*/
				thread->stats.FinishedOps(nullptr, 1, kWrite);
			} else if (query_type == 2) {		// Seek
				Iterator* iter = db_->NewIterator(options);
				iter->Seek(key);
				seek++;
				read++;
				if (iter->Valid() && iter->key() == key) seek_found++;
				delete iter;
				thread->stats.FinishedOps(nullptr, 1, kSeek);
			}
		}

		char msg[256];
		snprintf(msg, sizeof(msg), 
						 "( Gets: %ld, Puts: %ld, Seek: %ld of %ld in %ld found\n", 
						 gets, puts, seek, found, read);
		thread->stats.AddBytes(bytes);
		thread->stats.AddMessage(msg);

	}
#endif

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kRead);
#else
      thread->stats.FinishedSingleOp();
#endif
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kRead);
#else
      thread->stats.FinishedSingleOp();
#endif
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kRead);
#else
      thread->stats.FinishedSingleOp();
#endif
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d.", k);
      db_->Get(options, key, &value);
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kRead);
#else
      thread->stats.FinishedSingleOp();
#endif
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % range;
      snprintf(key, sizeof(key), "%016d", k);
      db_->Get(options, key, &value);
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kRead);
#else
      thread->stats.FinishedSingleOp();
#endif
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      iter->Seek(key);
      if (iter->Valid() && iter->key() == key) found++;
      delete iter;
#if MIXGRAPH
      thread->stats.FinishedOps(nullptr, 1, kSeek);
#else
      thread->stats.FinishedSingleOp();
#endif
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Delete(key);
#if MIXGRAPH
				thread->stats.FinishedOps(nullptr, 1, kDelete);
#else
        thread->stats.FinishedSingleOp();
#endif
      }
      s = db_->Write(write_options_, &batch);		// TODO db_->Put으로 고쳐야함
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
#if MIXGRAPH
      thread->stats.Start(thread->tid);
#else
      thread->stats.Start();
#endif
    }
  }

  void Compact(ThreadState* /*thread*/) {
    db_->CompactRange(NULL, NULL);
  }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
    WritableFile* file;
    Status s = Env::Default()->NewWritableFile(fname, &file);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      fprintf(stderr, "heap profiling not supported\n");
      Env::Default()->DeleteFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    uint64_t u64;
    int64_t i64;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
#if MIXGRAPH
    } else if (sscanf(argv[i], "--key_size=%d%c", &n, &junk) == 1) {
      FLAGS_key_size = n;
    } else if (sscanf(argv[i], "--writes=%d%c", &n, &junk) == 1) {
      FLAGS_writes = n;
		} else if (sscanf(argv[i], "-sine_a=%lf%c", &d, &junk) == 1) {
			FLAGS_sine_a = d;
		} else if (sscanf(argv[i], "-sine_b=%lf%c", &d, &junk) == 1) {
			FLAGS_sine_b = d;
		} else if (sscanf(argv[i], "-sine_c=%lf%c", &d, &junk) == 1) {
			FLAGS_sine_c = d;
		} else if (sscanf(argv[i], "-sine_d=%lf%c", &d, &junk) == 1) {
			FLAGS_sine_d = d;
		} else if (sscanf(argv[i], "-write_rate=%lf%c", &d, &junk) == 1) {
			FLAGS_write_rate = d;
		} else if (sscanf(argv[i], "-read_rate=%lf%c", &d, &junk) == 1) {
			FLAGS_read_rate = d;
		} else if (sscanf(argv[i], "-mix_get_ratio=%lf%c", &d, &junk) == 1) {
			FLAGS_mix_get_ratio = d;
		} else if (sscanf(argv[i], "-mix_put_ratio=%lf%c", &d, &junk) == 1) {
			FLAGS_mix_put_ratio = d;
		} else if (sscanf(argv[i], "-mix_seek_ratio=%lf%c", &d, &junk) == 1) {
			FLAGS_mix_seek_ratio = d;
		} else if (sscanf(argv[i], "-sine_mix_rate=%d%c", &n, &junk) == 1) {
			if (n) FLAGS_sine_mix_rate = true;
			else FLAGS_sine_mix_rate = false;
		} else if (sscanf(argv[i], "-sine_mix_rate_noise=%lf%c", &d, &junk) == 1) {
			FLAGS_sine_mix_rate_noise = d;
		} else if (sscanf(argv[i], "-sine_mix_rate_interval_milliseconds=%lu%c", &u64, &junk) == 1) {
			FLAGS_sine_mix_rate_interval_milliseconds = u64;
		} else if (sscanf(argv[i], "-report_interval_seconds=%ld%c", &i64, &junk) == 1) {
			FLAGS_report_interval_seconds = i64;
    } else if (strncmp(argv[i], "-report_file=", 13) == 0) {
      FLAGS_report_file = argv[i] + 13;
		} else if (sscanf(argv[i], "-stats_interval=%ld%c", &i64, &junk) == 1) {
			FLAGS_stats_interval = i64;
		} else if (sscanf(argv[i], "-stats_interval_seconds=%ld%c", &i64, &junk) == 1) {
			FLAGS_stats_interval_seconds = i64;
#endif
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }
#if MIXGRAPH
	if (FLAGS_stats_interval_seconds > 0)
		FLAGS_stats_interval = 1000;
#endif

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/dbbench";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
