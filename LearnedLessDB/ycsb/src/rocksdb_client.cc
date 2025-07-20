#include "rocksdb_client.h"
#include "algorithm"
#include "math.h"
#include "koo/koo.h"

namespace ycsbc {

#if YCSB_WRAPPER
RocksDBClient::RocksDBClient(WorkloadProxy* workload_proxy, int num_threads,
                             leveldb::Options options,
                             std::string data_dir,
                             leveldb::DB* db,
                             WorkloadWrapper* workload_wrapper) :
#else
RocksDBClient::RocksDBClient(WorkloadProxy* workload_proxy, int num_threads,
                             leveldb::Options options,
                             std::string data_dir,
                             leveldb::DB* db) :
#endif
    workload_proxy_(workload_proxy),
    num_threads_(num_threads),
    options_(options),
    data_dir_(data_dir),
    db_(db),
#if YCSB_WRAPPER
		workload_wrapper_(workload_wrapper),
#endif
    load_num_(workload_proxy_->record_count()),
#if YCSB_THROUGHPUTHIST
    request_num_(workload_proxy->operation_count()),
    stop_(false) {
#else
    request_num_(workload_proxy->operation_count()) {
#endif
    //total_finished_requests_(0),
    //total_write_latency(0),
    //total_read_latency(0),
    //write_finished(0),
    //read_finished(0) {
#if YCSB_THROUGHPUTHIST
  for (int i = 0; i < num_threads_; i++) {
    td_[i] = (thread_data*) aligned_alloc(64, sizeof(thread_data));
  }
#endif
#if !YCSB_COPYDB
  if (!db_) {
    //abort();
    leveldb::Status s = leveldb::DB::Open(options_, data_dir_, &db_);
    if (!s.ok()) {
      printf("%s\n", s.ToString().c_str());
      exit(0);
    }
    fprintf(stdout, "DB Opened rocksdb_client.cc\n");
    delete_db_ = true;
  }
#endif
  Reset();
}

RocksDBClient::~RocksDBClient() {
  if (delete_db_ && db_ != nullptr) delete db_;
#if !YCSB_WRAPPER
  if(request_time_ != nullptr)
    delete request_time_;
  if(read_time_ != nullptr)
    delete read_time_;
  if(update_time_ != nullptr)
    delete update_time_;
#endif
  if (workload_wrapper_ != nullptr)
    delete workload_wrapper_;
}

void RocksDBClient::run() { 
  if (workload_proxy_->is_load()) {
    Load();
  } else {
    Work();
  }
  //koo::Report();
  //koo::Reset();
}

void RocksDBClient::Load(){
	Reset();
#if !YCSB_WRAPPER
	assert(update_time_ == nullptr);
	update_time_ = new TimeRecord(load_num_ + 1);
#endif

	int base_coreid = 0;

	if (workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, load_num_, true);
	}

  // perfmon
#if YCSB_THROUGHPUTHIST
  std::thread perfmon_thread = std::thread(std::bind(&RocksDBClient::perfmon_loop, this));
#endif

	uint64_t num = load_num_ / num_threads_;
	std::vector<std::thread> threads;
	auto fn = std::bind(&RocksDBClient::RocksdDBLoader, this, 
						std::placeholders::_1, std::placeholders::_2);
	printf("start time: %s\n", GetDayTime().c_str());
	auto start = TIME_NOW;
	for(int i=0; i<num_threads_; i++){
		if(i == num_threads_ - 1)
			num = num + load_num_ % num_threads_;
		threads.emplace_back(fn, num, (base_coreid + i));
	}
	for(auto &t : threads) {
		t.join();
	}
	double time = TIME_DURATION(start, TIME_NOW);
	printf("end time: %s\n", GetDayTime().c_str());
#if YCSB_THROUGHPUTHIST
  stop_.store(true);
  if (perfmon_thread.joinable()) perfmon_thread.join();
#endif

	printf("==================================================================\n");
	PrintArgs();
	printf("Load %ld requests in %.3lf seconds.\n", load_num_, time/1000/1000);
#if !YCSB_WRAPPER
	printf("Load latency: %.3lf us\n", update_time_->Sum()/update_time_->Size());
	printf("Load median latency: %.3lf us\n", update_time_->Tail(0.5));
	printf("Load P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
			update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
		    update_time_->Tail(0.90), update_time_->Tail(0.75));
#endif
	printf("------------------------------------------------------------------\n");
  printf("Load %c - IOPS: %.3lf M\n", workload_proxy_->name_str().back(), load_num_/time*1000*1000/1000/1000);
	printf("==================================================================\n");
#if !YCSB_WRAPPER
	std::string stat_str2;
 	db_->GetProperty("leveldb.stats", &stat_str2);
 	printf("\n%s\n", stat_str2.c_str());
#endif
	fflush(stdout);

  // Record results to a file

#if !YCSB_WRAPPER
  update_time_->WriteToFile(workload_proxy_->filename_prefix(), "write_latency");
#endif
}

void RocksDBClient::Work(){
	Reset();
#if !YCSB_WRAPPER
	assert(request_time_ == nullptr);
	assert(read_time_ == nullptr);
	assert(update_time_ == nullptr);
	request_time_ = new TimeRecord(request_num_ + 1);
	read_time_ = new TimeRecord(request_num_ + 1);
	update_time_ = new TimeRecord(request_num_ + 1);
#endif

	int base_coreid = 0;

	if (workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, request_num_, false);
	}

  // perfmon
#if YCSB_THROUGHPUTHIST
  std::thread perfmon_thread = std::thread(std::bind(&RocksDBClient::perfmon_loop, this));
#endif

	uint64_t num = request_num_ / num_threads_;
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool)> fn;
  fn = std::bind(&RocksDBClient::RocksDBWorker, this, 
              std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3);
	printf("start time: %s\n", GetDayTime().c_str());
	auto start = TIME_NOW;
	for(int i=0; i<num_threads_; i++){
		if(i == num_threads_ - 1)
			num = num + request_num_ % num_threads_;
		threads.emplace_back(fn, num, (base_coreid + i), i==0);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	printf("end time: %s\n", GetDayTime().c_str());
#if YCSB_THROUGHPUTHIST
  stop_.store(true);
  if (perfmon_thread.joinable()) perfmon_thread.join();
#endif

#if !YCSB_WRAPPER
	std::string stat_str2;
	db_->GetProperty("leveldb.stats", &stat_str2);
	printf("\n%s\n", stat_str2.c_str());
	fflush(stdout);

	assert(request_time_->Size() == request_num_);
#endif
	printf("==================================================================\n");
#if !YCSB_WRAPPER
	PrintArgs();
	printf("WAL sync time per request: %.3lf us\n", wal_time_/request_num_);
	//printf("WAL sync time per sync: %.3lf us\n", wal_time_/
	//	   							options_.statistics->getTickerCount(rocksdb::WAL_FILE_SYNCED));
	printf("Wait time: %.3lf us\n", wait_time_/request_num_);
	//SPANDB:printf("Complete wait time: %.3lf us\n", complete_memtable_time_/request_num_);
	printf("Write delay time: %.3lf us\n", write_delay_time_/request_num_);
	printf("Write memtable time: %.3lf\n", write_memtable_time_/request_num_);
#endif
	printf("Finish %ld requests in %.3lf seconds.\n", request_num_, time/1000/1000);
#if !YCSB_WRAPPER
	if(read_time_->Size() != 0){
		printf("read num: %ld, read avg latency: %.3lf us, read median latency: %.3lf us\n", 
				read_time_->Size(), read_time_->Sum()/read_time_->Size(), read_time_->Tail(0.50));
		printf("read P999: %.3lf us, P99: %.3lf us, P95: %.3lf us, P90: %.3lf us, P75: %.3lf us\n",
				read_time_->Tail(0.999), read_time_->Tail(0.99), read_time_->Tail(0.95),
				read_time_->Tail(0.90), read_time_->Tail(0.75));
	}else{
		printf("read num: 0, read avg latency: 0 us, read median latency: 0 us\n");
		printf("read P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	if(update_time_->Size() != 0){
		printf("update num: %ld, update avg latency: %.3lf us, update median latency: %.3lf us\n", 
			    update_time_->Size(), update_time_->Sum()/update_time_->Size(), update_time_->Tail(0.50));
		printf("update P999: %.3lf us, P99: %.3lf us, P95: %.3lfus, P90: %.3lf us, P75: %.3lf us\n",
				update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
				update_time_->Tail(0.90), update_time_->Tail(0.75));
	}else{
		printf("update num: 0, update avg latency: 0 us, update median latency: 0 us\n");
		printf("update P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	printf("Work latency: %.3lf us\n", request_time_->Sum()/request_time_->Size());
	printf("Work IOPS: %.3lf M\n", request_num_/time*1000*1000/1000/1000);
	printf("Work median latency: %.3lf us\n", request_time_->Tail(0.5));
	printf("Work P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
			request_time_->Tail(0.999), request_time_->Tail(0.99), request_time_->Tail(0.95),
		    request_time_->Tail(0.90), request_time_->Tail(0.75));
	//printf("Stall: %.3lf us\n", options_.statistics->getTickerCount(rocksdb::STALL_MICROS)*1.0);
	//printf("Stall rate: %.3lf \n", options_.statistics->getTickerCount(rocksdb::STALL_MICROS)*1.0/time);
	printf("Block read time: %.3lf us\n", block_read_time_/read_time_->Size());
	//uint64_t block_hit = options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
	//uint64_t block_miss = options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
	//uint64_t memtable_hit = options_.statistics->getTickerCount(rocksdb::MEMTABLE_HIT);
	//uint64_t memtable_miss = options_.statistics->getTickerCount(rocksdb::MEMTABLE_MISS);
	//printf("block cache hit ratio: %.3lf (hit: %ld, miss: %ld)\n", 
	//		block_hit*1.0/(block_hit+block_miss), block_hit, block_miss);
	//printf("memtable hit ratio: %.3lf (hit: %ld, miss: %ld)\n",
	//	   memtable_hit*1.0/(memtable_hit+memtable_miss), memtable_hit, memtable_miss);
	printf("submit_time: %.3lf\n", submit_time_ / 1000.0 / request_num_);
#endif
	printf("------------------------------------------------------------------\n");
  printf("%s %s - IOPS: %.3lf M\n", workload_proxy_->name_str().c_str(), workload_proxy_->distribution_str().c_str(), request_num_/time*1000*1000/1000/1000);
#if YCSB_WRAPPER
	printf("Read proportion: %f, Update proportion: %f\n", workload_proxy_->read_proportion, workload_proxy_->update_proportion);
	printf("time: %s\n", GetDayTime().c_str());
#endif
	printf("==================================================================\n");
	fflush(stdout);

#if !YCSB_WRAPPER
  request_time_->WriteToFile(workload_proxy_->filename_prefix(), "request_latency");
  update_time_->WriteToFile(workload_proxy_->filename_prefix(), "write_latency");
  read_time_->WriteToFile(workload_proxy_->filename_prefix(), "read_latency");
#endif
}

void RocksDBClient::RocksDBWorker(uint64_t num, int coreid, bool is_master){
  //auto mybuf = aligned_alloc(64, sizeof(thread_data));
  //memset(mybuf, 0, sizeof(thread_data));
  //td_[coreid] = (thread_data*) mybuf;
	// SetAffinity(coreid);
	//rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	//rocksdb::get_perf_context()->Reset();
	//rocksdb::get_iostats_context()->Reset();
#if !YCSB_WRAPPER
	TimeRecord request_time(num + 1);
	TimeRecord read_time(num + 1);
	TimeRecord update_time(num + 1);
#endif

	if(is_master){
		printf("starting requests...\n");
	}

  //static char w_value_buf[4096];
  //memset(w_value_buf, 'a', 4096);
  static char w_value_buf[4096];
  for (int i=0; i<4096; i++) w_value_buf[i] = (char)i;
  //RandomGenerator gen;
  std::string r_value;

	for(uint64_t i=0; i<num; i++){
		WorkloadWrapper::Request *req = workload_wrapper_->GetNextRequest();
		ycsbc::Operation opt = req->Type();
		assert(req != nullptr);
#if !YCSB_WRAPPER
		auto start = TIME_NOW;
#endif
		if(opt == READ){
			//db_->Get(read_options_, req->Key(), &r_value);
			//ERR(db_->Get(read_options_, req->Key(), &r_value));
			leveldb::Status s = db_->Get(read_options_, req->Key(), &r_value);
			if (!s.ok()) fprintf(stdout, "\tREAD search key %s Not Found\n", req->Key().c_str());
			//fprintf(stderr, "\tsearch key: %s\n", req->Key().c_str());
		}else if(opt == UPDATE){
      leveldb::Slice w_value(w_value_buf, req->Length());
			ERR(db_->Put(write_options_, req->Key(), w_value));
			//ERR(db_->Put(write_options_, req->Key(), gen.Generate(req->Length())));
		}else if(opt == INSERT){
      leveldb::Slice w_value(w_value_buf, req->Length());
			ERR(db_->Put(write_options_, req->Key(), w_value));
			//ERR(db_->Put(write_options_, req->Key(), gen.Generate(req->Length())));
		}else if(opt == READMODIFYWRITE){
			//ERR(db_->Get(read_options_, req->Key(), &r_value));
			leveldb::Status s = db_->Get(read_options_, req->Key(), &r_value);
			if (!s.ok()) fprintf(stdout, "\tREADMODIFYWRITE search key %s Not Found\n", req->Key().c_str());
      leveldb::Slice w_value(w_value_buf, req->Length());
			ERR(db_->Put(write_options_, req->Key(), w_value));
			//ERR(db_->Put(write_options_, req->Key(), gen.Generate(req->Length())));
		}else if(opt == SCAN){
			leveldb::Iterator* iter = db_->NewIterator(read_options_);
			iter->Seek(req->Key());
			for (int i = 0; i < req->Length() && iter->Valid(); i++) {
				// Do something with it->key() and it->value().
        		iter->Next();
    		}
    		ERR(iter->status());
    		delete iter;
		}else{
			throw utils::Exception("Operation request is not recognized!");
		}
#if !YCSB_WRAPPER
		double time =  TIME_DURATION(start, TIME_NOW);
		request_time.Insert(time);
#endif
		if(opt == READ || opt == SCAN){
#if !YCSB_WRAPPER
			read_time.Insert(time);
			//total_read_latency.fetch_add((uint64_t)time);
			//read_finished.fetch_add(1);
#endif
#if YCSB_THROUGHPUTHIST
      td_[coreid]->total_finished_requests++;
      td_[coreid]->read_finished++;
#endif
		}else if(opt == UPDATE || opt == INSERT || opt == READMODIFYWRITE){
#if !YCSB_WRAPPER
			update_time.Insert(time);
			//total_write_latency.fetch_add((uint64_t)time);
			//write_finished.fetch_add(1);
#endif
#if YCSB_THROUGHPUTHIST
      td_[coreid]->total_finished_requests++;
      td_[coreid]->write_finished++;
#endif
		}else{
			assert(0);
		}
		//total_finished_requests_.fetch_add(1);
	}

  //td_[coreid] = NULL;
  //free(mybuf);
#if !YCSB_WRAPPER
	mutex_.lock();
	request_time_->Join(&request_time);
	read_time_->Join(&read_time);
	update_time_->Join(&update_time);
	//wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	//wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	//SPANDB:complete_memtable_time_ += rocksdb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	//write_delay_time_ += rocksdb::get_perf_context()->write_delay_time/1000.0;
	//block_read_time_ += rocksdb::get_perf_context()->block_read_time/1000.0;
	//write_memtable_time_ += rocksdb::get_perf_context()->write_memtable_time/1000.0;
	//printf("%s\n\n",rocksdb::get_perf_context()->ToString().c_str());
	//printf("%s\n\n",rocksdb::get_iostats_context()->ToString().c_str());
	mutex_.unlock();
#endif
}

void RocksDBClient::RocksdDBLoader(uint64_t num, int coreid){
  //auto mybuf = aligned_alloc(64, sizeof(thread_data));
  //memset(mybuf, 0, sizeof(thread_data));
  //td_[coreid] = (thread_data*) mybuf;
  //memset(td_[coreid], 0, sizeof(thread_data));
	//rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	//rocksdb::get_perf_context()->Reset();
	//rocksdb::get_iostats_context()->Reset();

#if !YCSB_WRAPPER
	TimeRecord request_time(num + 1);
	TimeRecord update_time(num + 1);
#endif

  //static char w_value_buf[4096];			// TODO !!!! value compression?
  //memset(w_value_buf, 'a', 4096);
	static char w_value_buf[4096];
	for (int i=0; i<4096; i++) w_value_buf[i] = (char)i;
  //RandomGenerator gen;								// db_bench value generator

	for(uint64_t i=0; i<num; i++){		
		std::string key;
		WorkloadWrapper::Request *req = workload_wrapper_->GetNextRequest();
		/*ycsbc::Operation opt = req->Type();
		assert(req != nullptr);
    assert(opt == INSERT);*/
#if !YCSB_WRAPPER
		auto start = TIME_NOW;
#endif
    leveldb::Slice w_value(w_value_buf, req->Length());
    ERR(db_->Put(write_options_, req->Key(), w_value));
    //ERR(db_->Put(write_options_, req->Key(), gen.Generate(req->Length())));
//fprintf(stderr, "load key: %s\n", req->Key().c_str());
#if !YCSB_WRAPPER
    double time = TIME_DURATION(start, TIME_NOW);
    request_time.Insert(time);
    update_time.Insert(time);
    ///total_write_latency.fetch_add((uint64_t)time);
    ///write_finished.fetch_add(1);
#endif
#if YCSB_THROUGHPUTHIST
    td_[coreid]->total_finished_requests++;
    td_[coreid]->write_finished++;
#endif
	}
  //td_[coreid] = NULL;
  //free(mybuf);
#if !YCSB_WRAPPER
	mutex_.lock();
	update_time_->Join(&update_time);
	//wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	//wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	//SPANDB:complete_memtable_time_ += rocksdb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	//write_delay_time_ += rocksdb::get_perf_context()->write_delay_time/1000.0;
	//write_memtable_time_ += rocksdb::get_perf_context()->write_memtable_time/1000.0;
	mutex_.unlock();
#endif
}

void RocksDBClient::Reset(){
	//options_.statistics->Reset();
	//db_->ResetStats();
#if !YCSB_WRAPPER
	wait_time_ = wal_time_ = 0;
	//SPANDB:complete_memtable_time_ = 0;
	block_read_time_ = 0;
	write_delay_time_ = 0;
	write_memtable_time_ = 0;
	submit_time_ = 0;
	//total_finished_requests_.store(0);
#endif

#if YCSB_THROUGHPUTHIST
  for (int i = 0; i < num_threads_; i++) {
    if (td_[i]) {
      memset(td_[i], 0, sizeof(thread_data));
    }
  }
  stop_.store(false);
#endif
}

void RocksDBClient::PrintArgs(){
	printf("-----------configuration------------\n");
  printf("Clients: %d\n", num_threads_);
	//printf("WAL: %d, fsync: %d\n", !(write_options_.disableWAL), write_options_.sync);
	printf("Data: %s\n", data_dir_.c_str());
	/*if(write_options_.disableWAL){
		printf("WAL: nologging\n");
	}else{
		printf("WAL: %s\n", options_.wal_dir.c_str());
	}
	printf("Max_write_buffer_number: %d\n", options_.max_write_buffer_number);
	printf("Max_background_jobs: %d\n", options_.max_background_jobs);
	printf("High-priority backgd threds: %d\n", options_.env->GetBackgroundThreads(leveldb::Env::HIGH));
	printf("Low-priority backgd threds: %d\n", options_.env->GetBackgroundThreads(leveldb::Env::LOW));
	printf("Max_subcompactions: %d\n", options_.max_subcompactions);
	if(options_.write_buffer_size >= (1ull << 30)){
		printf("Write_buffer_size: %.3lf GB\n", options_.write_buffer_size * 1.0/(1ull << 30));
	}else{
		printf("Write_buffer_size: %.3lf MB\n", options_.write_buffer_size * 1.0/(1ull << 20));
	}*/
	printf("-------------------------------------\n");
	//printf("write done by self: %ld\n", options_.statistics->getTickerCount(rocksdb::WRITE_DONE_BY_SELF));
	//printf("WAL sync num: %ld\n", options_.statistics->getTickerCount(rocksdb::WAL_FILE_SYNCED));
	//printf("WAL write: %.3lf GB\n", options_.statistics->getTickerCount(rocksdb::WAL_FILE_BYTES)*1.0/(1ull << 30));
	//printf("compaction read: %.3lf GB, compaction write: %.3lf GB\n",
	//		options_.statistics->getTickerCount(rocksdb::COMPACT_READ_BYTES)*1.0/(1ull << 30),
	//		options_.statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES)*1.0/(1ull << 30));
	//printf("flush write: %.3lf GB\n",
	//		options_.statistics->getTickerCount(rocksdb::FLUSH_WRITE_BYTES)*1.0/(1ull << 30));
	//printf("flush time: %.3lf s\n",
	//		options_.statistics->getTickerCount(rocksdb::FLUSH_TIME)*1.0/1000000);
#if !YCSB_WRAPPER
	fflush(stdout);
#endif
}

void RocksDBClient::perfmon_loop() {
#if YCSB_THROUGHPUTHIST
  char user_name[100];
  getlogin_r(user_name, 100);
  std::string logdir("/scratch/");
  logdir.append(user_name).append("/logs/leveldb/");
  fs::create_directories(logdir);

  std::string logpath = logdir + workload_proxy_->filename_prefix() + "." + "throughput_history";
  std::string finalpath = logpath;
  int num = 0;
  while (fs::exists(finalpath)) {
    finalpath = logpath + "." + std::to_string(num++);
  }
  std::ofstream outfile(finalpath);

  auto tp_begin = std::chrono::steady_clock::now();
  auto tp_last = tp_begin;
  uint64_t total_cnt_last = 0;
  uint64_t write_cnt_last = 0;
  uint64_t read_cnt_last = 0;
  double total_throughput = 0;
  double write_throughput = 0;
  double read_throughput = 0;

  while (!stop_.load()) {
    // timestamp
    auto tp_now = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur_from_begin = tp_now - tp_begin;
    std::chrono::duration<double> dur_from_last = tp_now - tp_last;
    double timestamp = dur_from_begin.count();
    double dur = dur_from_last.count();
     
    // Client Queries
    uint64_t total_cnt_now = 0;
    uint64_t write_cnt_now = 0;
    uint64_t read_cnt_now = 0;
    for (int i = 0; i < num_threads_; i++) {
      if (td_[i]) {
        total_cnt_now += td_[i]->total_finished_requests;
        write_cnt_now += td_[i]->write_finished;
        read_cnt_now += td_[i]->read_finished;
      }
    }
    uint64_t total_cnt_inc = total_cnt_now - total_cnt_last;
    uint64_t write_cnt_inc = write_cnt_now - write_cnt_last;
    uint64_t read_cnt_inc = read_cnt_now - read_cnt_last;
    total_throughput = (double) total_cnt_inc / dur;
    write_throughput = (double) write_cnt_inc / dur;
    read_throughput = (double) read_cnt_inc / dur;
     
    static char writebuf[200];
		time_t timer = time(NULL);
		struct tm* t = localtime(&timer);
    sprintf(writebuf, "%d:%d:%d *** timestamp: %.3lf - Throughput: %.3lf Mops/s (Write: %.3lf Mops/s, Read: %.3lf Mops/s) ***\n", t->tm_hour, t->tm_min, t->tm_sec, timestamp, total_throughput/1000/1000, write_throughput/1000/1000, read_throughput/1000/1000);
    outfile << writebuf;

    tp_last = tp_now;
    total_cnt_last = total_cnt_now;
    write_cnt_last = write_cnt_now;
    read_cnt_last = read_cnt_now;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  outfile.flush();
  outfile.close();
  fprintf(stdout, "Throughput history is written to: %s\n", finalpath.c_str());
#endif
}

}
