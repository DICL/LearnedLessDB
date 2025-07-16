//
// Created by daiyi on 2020/02/02.
//

#include "koo/learned_index.h"

#include "db/version_set.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <utility>
#include "util/mutexlock.h"
#include "koo/util.h"
#include "koo/koo.h"
#include "koo/extenthash.h"

namespace koo {

#if LEARN

std::pair<uint64_t, uint64_t> LearnedIndexData::GetPosition(
#if MERGE
    Slice& target_x) {
#else
    const Slice& target_x) const {
#endif
  assert(string_segments.size() > 1);

  // check if the key is within the model bounds
#if YCSB_CXX
  uint64_t target_int = target_x.SliceToInteger();
#else
  uint64_t target_int = SliceToInteger(target_x);
#endif
  if (target_int > max_key) return std::make_pair(size, size);
  if (target_int < min_key) return std::make_pair(size, size);

#if EXTENT_HASH
#if EH_AC_TEST
	koo::num_eh_get_total++;
#endif
#if EH_TIME_R
	std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
	uint32_t left = 0;
	if (Hashed() && GetExtent(target_int, left)) {
		// Succeed
#if EH_TIME_R
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		koo::eh_get_time += nano.count();
		koo::eh_get_num++;
#endif
#if EH_AC_TEST
		koo::num_eh_get++;
#endif
#if EXTENT_HASH_DEBUG
		std::ofstream ofs("/koo/Extent-hashing/linears/pm/data_ycsb/eh_get_time.txt", std::ios::app);
		if (merge_history) ofs << file_number << " " << target_int << " 1\n";
		else ofs << file_number << " " << target_int << " 0\n";;
		ofs.close();
#endif
	} else {		// Binary search
		uint32_t right = (uint32_t)string_segments.size() - 1;
		while (left != right - 1) {
			uint32_t mid = (right + left) / 2;
	    if (target_int < string_segments[mid].x) right = mid;
		  else left = mid;
		}
#if EH_TIME_R
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		koo::bi_get_time += nano.count();
		koo::bi_get_num++;
#endif
	}
#else		// EXTENT_HASH
#if EH_TIME_R
	std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
  // binary search between segments
  uint32_t left = 0, right = (uint32_t)string_segments.size() - 1;
  while (left != right - 1) {
    uint32_t mid = (right + left) / 2;
    if (target_int < string_segments[mid].x) right = mid;
    else left = mid;
  }
#if EH_TIME_R
	std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
	koo::bi_get_time += nano.count();
	koo::bi_get_num++;
#endif
#endif
  
  // 나는 x_last도 저장 -> target_int > seg[left].x_last면 없는 키로 바로 반환 가능? 여기에 x보다 1 작은키 버그도 처리?
  // TODO merged model은 해당 안됨. GetPosition 함수 따로 만들까
  // 이상하게 x_last보다 1 큰키 찾는 경우도 있네;;
  /*if (target_int-1 > string_segments[left].x_last) {
  	if (target_int+1 == string_segments[left+1].x) left++;
  	else return std::make_pair(size, size);
	}*/
	
	// TODO 확인!!
	/*if (!(string_segments[left].x <= target_int && target_int <= string_segments[left].x_last))
		return std::make_pair(size, size);*/

  // calculate the interval according to the selected segment
  double result =
      target_int * string_segments[left].k + string_segments[left].b;
#if RETRAIN2
	double error = GetError();
#endif
  uint64_t lower =
      result - error > 0 ? (uint64_t)std::floor(result - error) : 0;
  uint64_t upper = (uint64_t)std::ceil(result + error);
	upper = upper < size ? upper : size - 1;
#if MERGE
	if (lower >= size) {
#if MAX_MERGE_HISTORY
		if (merge_history) lower = upper - 2*error - 1;
		//if (merge_history) lower = upper - 2*MERGE_MODEL_ERROR - 1;
#else
		if (merged) lower = upper - 2*error - 1;
		//if (merged) lower = upper - 2*MERGE_MODEL_ERROR - 1;
#endif
		else return std::make_pair(size, size);
	}
	/*if (Merged()) {				// for num10
		if (lower >= size) lower = upper - 2*MERGE_MODEL_ERROR - 1;
	} else {
	  if (lower >= size) return std::make_pair(size, size);
	}*/
#endif

#if DEBUG
	//if (target_int < string_segments[left].x) std::cout << "1: " << target_int << " " << string_segments[left].x << " " << left << std::endl;
	//else if (string_segments[left].x_last < target_int) std::cout << "2: " << string_segments[left].x_last << " " << target_int << " " << left << std::endl;
	/*if (target_int+1 == string_segments[left+1].x) {
		std::cout << "target = " << target_int << ", " << file_number << std::endl;
		std::cout << left << ": " << string_segments[left].x << " ~ " << std::endl;
		std::cout << left+1 << ": " << string_segments[left+1].x << " ~ " << std::endl;
		std::cout << std::endl;
	}*/
#endif
#if OPT2
	if (!merged) {
		if (left && lower <= string_segments[left-1].y_last) {
			//std::cout << "lower: " << lower << " " << string_segments[left-1].y_last << std::endl;
			lower = string_segments[left-1].y_last + 1;
		}
		if (upper > string_segments[left].y_last) {
			//std::cout << "upper: " << upper << " " << string_segments[left].y_last << std::endl;
			upper = string_segments[left].y_last;
		}
	}
#endif
  return std::make_pair(lower, upper);
}

uint64_t LearnedIndexData::MaxPosition() const { return size - 1; }

double LearnedIndexData::GetError() const { return error; }

#if RETRAIN2
void LearnedIndexData::SetError(double cur_error, uint64_t extra_error) { 
	if (cur_error == error ||
			cur_error + extra_error > error) {
		error = cur_error + extra_error;
#if AC_TEST
		//actual_extended_cnt++;
#endif
	}
}
#endif

#if MERGE
#if MAX_MERGE_HISTORY
void LearnedIndexData::SetMergedModel(std::vector<Segment>& segs, uint32_t history) {
	merge_history = history;
#else
void LearnedIndexData::SetMergedModel(std::vector<Segment>& segs) {
	merged = true;
#endif
	error = MERGE_MODEL_ERROR;
	string_segments = std::move(segs);
	learned.store(true);

#if EXTENT_HASH
	size_t segs_size = string_segments.size() - 1;		// dummy segment
	if (segs_size < EH_MAX_NUM_SEG) {
#if EH_TIME_R
		std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
		uint64_t cont_x = string_segments[0].x;
		bool result = true;
		for (int i=0; i<segs_size; i++) {
			if (!InsertExtent(Extent(cont_x, string_segments[i].x_last - cont_x + 1), i)) {
				result = false;
				break;
			}
			//if (cont_x/100000000 != cont_x/(uint64_t)(std::pow(10, PLR_POINT))) std::cout << "Wrong!!\n";
			cont_x = string_segments[i].x_last + 1;
		}
		if (result) {
			hashed.store(true);
			hashed_not_atomic = true;
#if EH_AC_TEST
			koo::num_eh_insert++;
#endif
		}
#if EH_AC_TEST
		else { koo::num_eh_insert_fail++; }
#endif
#if EH_TIME_R
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		koo::eh_insert_time += nano.count();
		koo::eh_insert_num++;
		if (!result) koo::eh_insert_full++;
#endif
#if EXTENT_HASH_DEBUG
		std::ofstream ofs("/koo/Extent-hashing/linears/pm/data_ycsb/merged/segs_"+std::to_string(file_number)+".txt");
		cont_x = string_segments[0].x;
		for (auto& s : string_segments) {
			ofs << cont_x << " " << s.x_last << std::endl;
			cont_x = s.x_last + 1;
		}
		ofs.close();
#endif
	}
#if EH_TIME_R
	else koo::eh_insert_toomanysegs++;
#endif
#endif

#if DEBUG
  //if (fresh_write) {			// TODO	DB close할때 쓰게 할까?
	  //WriteModel(koo::db->versions_->dbname_ + "/" + to_string(file_number) + ".fmodel");
		//self->num_entries_accumulated.array.clear();
  //}
#endif
	return;
}

#if MAX_MERGE_HISTORY
bool LearnedIndexData::CheckMergeHistory() {
	if (merge_history >= MAX_MERGE_HISTORY) return false;
	return true;
}

uint32_t LearnedIndexData::GetMergeHistory() {
	return merge_history;
}
#else
bool LearnedIndexData::Merged() {
	return merged;
}
#endif

#if RETRAIN
void LearnedIndexData::FreezeModel() {
	if (learned_not_atomic) {
		learned.store(false);
		learned_not_atomic = false;
	}
}

void LearnedIndexData::UnfreezeModel() {
	learned.store(true);
}

bool LearnedIndexData::SetRetraining() {
	//if (retraining.load()) return false;
	bool try_retraining = false;
	if (!retraining.compare_exchange_weak(try_retraining, !try_retraining)) return false;
	//bool test = retraining.load();
	//if (!retraining.compare_exchange_weak(test, !test)) return false;
	
	/*learned.store(false);
	learned_not_atomic = false;*/
	//error = file_model_error;
	//merged.store(false);

  if (string_segments.size() > 0 && !is_replaced_) {
    //std::unique_lock<SpinLock> lock(string_segments_lock_);
    string_segments_bak = std::move(string_segments);
    is_replaced_ = true;
  }

#if EXTENT_HASH
	hashed.store(false);
	hashed_not_atomic = false;
#endif
	return true;
}
#endif
#endif

#if EXTENT_HASH
bool LearnedIndexData::Hashed() {
	if (hashed_not_atomic) return true;
	else if (hashed.load()) {
		hashed_not_atomic = true;
		return true;
	}
	return false;
}

void LearnedIndexData::InitBuckets() {
	for (int i=0; i<BUCKET_LEN; i++)
		buckets[i].empty = true;
}

bool LearnedIndexData::InsertExtentImpl(uint64_t bucket_key, Extent& e, uint32_t value) {
	uint64_t bucket_idx = EH_HASH(bucket_key);

	if (!buckets[bucket_idx].empty) {		// linear probing
		uint64_t linear_idx = (bucket_idx + 1) % BUCKET_LEN;
		do {
			if (buckets[linear_idx].empty) {
				bucket_idx = linear_idx;
				break;
			}
			linear_idx = (linear_idx + 1) % BUCKET_LEN;
		} while (linear_idx != bucket_idx);
	}
	if (!buckets[bucket_idx].empty) {
		std::cout << BUCKET_LEN << " Buckets Full!!!!! #segs: " << string_segments.size() << std::endl;
		return false;
	}

	buckets[bucket_idx].empty = false;
	buckets[bucket_idx].key_x = e.lcn;										// x
	buckets[bucket_idx].key_x_last = e.lcn + e.len - 1;		// x_last
	buckets[bucket_idx].seg_num = value;

	bucket_key += calc_stride_len(bucket_key, e.len >> EH_N);
	if (bucket_key > (e.lcn + e.len - 1) >> EH_N) return true;
	return InsertExtentImpl(bucket_key, e, value);
}

bool LearnedIndexData::InsertExtent(Extent e, uint32_t value) {
	//std::cout << e.lcn << " " << (e.lcn>>EH_N) << std::endl;
	return InsertExtentImpl(e.lcn >> EH_N, e, value);
}

bool LearnedIndexData::GetExtent(uint64_t k, uint32_t& v) {
	uint64_t mask = MASK(k >> EH_N);
	uint64_t key = k >> EH_N;

	while (true) {
		uint64_t bucket_idx = EH_HASH(key);
		if (!buckets[bucket_idx].empty) {
			if (k >= buckets[bucket_idx].key_x && k <= buckets[bucket_idx].key_x_last) {
				v = buckets[bucket_idx].seg_num;
				return true;
			}

			uint64_t linear_idx = (bucket_idx + 1) % BUCKET_LEN;		// linear probing
			do {
				if (buckets[linear_idx].empty) break;
				if (k >= buckets[linear_idx].key_x && k <= buckets[linear_idx].key_x_last) {
					v = buckets[linear_idx].seg_num;
					return true;
				}

				linear_idx = (linear_idx + 1) % BUCKET_LEN;
			} while (linear_idx != bucket_idx);
		}

		if (!key) break;		// NotFound
		key &= ((mask << (ffs(key))) & mask);
	}

#if EH_AC_TEST
	koo::num_eh_get_fail++;
#endif
	return false;
}
/*bool LearnedIndexData::GetExtent(uint64_t k, uint32_t& v) {		// log stride first
	uint64_t mask = MASK(k >> EH_N);
	uint64_t key = k >> EH_N;
	std::vector<std::pair<uint64_t, uint64_t>> log_infos;		// <bucket_idx, key> for linear probing after log stride search

	while (true) {		// log stride search
		uint64_t bucket_idx = EH_HASH(key);
		if (!buckets[bucket_idx].empty) {
			if (k >= buckets[bucket_idx].key_x && k <= buckets[bucket_idx].key_x_last) {
				v = buckets[bucket_idx].seg_num;
				return true;
			}
			log_infos.push_back(std::make_pair(bucket_idx, key));
		}
		if (!key) break;		// NotFound
		key &= ((mask << (ffs(key))) & mask);
	}

	for (auto& log_info : log_infos) {		// linear probing search TODO 뒤에서부터?
		uint64_t bucket_idx = log_info.first;
		uint64_t linear_idx = (bucket_idx + 1) % BUCKET_LEN;
		key = log_info.second;
		do {
			if (buckets[linear_idx].empty) break;
			if (k >= buckets[linear_idx].key_x && k <= buckets[linear_idx].key_x_last) {
				v = buckets[linear_idx].seg_num;
				return true;
			}
			linear_idx = (linear_idx + 1) % BUCKET_LEN;
		} while (linear_idx != bucket_idx);
	}
#if EH_AC_TEST
	koo::num_eh_get_fail++;
#endif
	return false;
}*/
#endif

// Actual function doing learning
bool LearnedIndexData::Learn() {
  // FILL IN GAMMA (error)
  PLR plr = PLR(LEARN_MODEL_ERROR);

  // check if data if filled
#if RETRAIN
	if (string_keys->empty()) {		// TODO 파일이 compaction input으로 들어가서, 또 왜? <- 빼도 될듯?
		std::cout << "string_keys.empty()" << std::endl;
		string_keys->shrink_to_fit();
		return false;
	}
#else
  if (string_keys->empty()) assert(false);
#endif

  // fill in some bounds for the model
#if YCSB_CXX
  //min_key = (uint64_t) std::stoull(string_keys.front());
  //max_key = (uint64_t) std::stoull(string_keys.back());
	min_key = string_keys->front();
	max_key = string_keys->back();
#else
	min_key = atoll(string_keys->front().c_str());
	max_key = atoll(string_keys->back().c_str());
#endif
  size = string_keys->size();
#if TIME_W
	koo::learn_size += size;
	koo::num_learn_size++;
#endif

  // actual training
  std::vector<Segment> segs = plr.train(*string_keys);
  if (segs.empty()) return false;
  if (Deleted()) return false;
#if MERGE
	if (segs.front().x != min_key) segs.front().x = min_key;
	if (segs.back().x_last != max_key) segs.back().x_last = max_key;
#endif
  // fill in a dummy last segment (used in segment binary search)
  segs.push_back((Segment){max_key, 0, 0, 0, 0});

 /* if (string_segments.size() > 0 && !is_replaced_) {
    std::unique_lock<SpinLock> lock(string_segments_lock_);
    is_replaced_ = true;
    string_segments_bak = std::move(string_segments);
  }*/
  string_segments = std::move(segs);

#if RETRAIN
	if (retraining.load()) {
		error = LEARN_MODEL_ERROR;
#if MAX_MERGE_HISTORY
		merge_history = 0;
#else
		merged = false;
#endif
		//retraining.store(false);
#if EXTENT_HASH
		InitBuckets();
#endif
#if AC_TEST
		koo::num_retrained++;
#endif
	}
#endif
	learned.store(true);
#if EXTENT_HASH
	size_t segs_size = string_segments.size() - 1;			// dummy segment
	if (segs_size < EH_MAX_NUM_SEG) {
#if EH_TIME_R
		std::chrono::system_clock::time_point StartTime = std::chrono::system_clock::now();
#endif
		uint64_t cont_x = string_segments[0].x;
		bool result = true;
		for (int i=0; i<segs_size; i++) {
			if (!InsertExtent(Extent(cont_x, string_segments[i].x_last - cont_x + 1), i)) {
				result = false;
				break;
			}
			cont_x = string_segments[i].x_last + 1;
		}
		if (result) {
			hashed.store(true);
			hashed_not_atomic = true;
#if EH_AC_TEST
			koo::num_eh_insert++;
#endif
		}
#if EH_AC_TEST
		else { koo::num_eh_insert_fail++; }
#endif
#if EH_TIME_R
		std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
		koo::eh_insert_time += nano.count();
		koo::eh_insert_num++;
		if (!result) koo::eh_insert_full++;
#endif
#if EXTENT_HASH_DEBUG
		std::ofstream ofs("/koo/Extent-hashing/linears/pm/data_ycsb/learned/segs_"+std::to_string(file_number)+".txt");
		cont_x = string_segments[0].x;
		for (auto& s : string_segments) {
			ofs << cont_x << " " << s.x_last << std::endl;
			cont_x = s.x_last + 1;
		}
		ofs.close();
#endif
	}
#if EH_TIME_R
	else koo::eh_insert_toomanysegs++;
#endif
#endif

#if DEBUG
	/*std::ofstream of_keys("/koo/HyperLearningless/koo/data/keys_" + std::to_string(file_number) + "_learned.txt");
	for (auto& key : string_keys)
		of_keys << key << "\n";
	of_keys.close();*/
	std::ofstream of_segs("/koo/HyperLearningless/koo/data/segs_" + std::to_string(file_number) + "_learned.txt");
	of_segs.precision(15);
	for (auto& s : string_segments)
		of_segs << "(" << s.x << ", )~(" << s.x_last << ", " << s.y_last << "): y = " << s.k << " * x + " << s.b << "\n";
	of_segs.close();
#endif
  //string_keys.clear();		// TODO 없어도 될듯?

  return true;
}

// static learning function to be used with LevelDB background scheduling
// file learning
uint64_t LearnedIndexData::FileLearn(void* arg) {
  MetaAndSelf* mas = reinterpret_cast<MetaAndSelf*>(arg);
  LearnedIndexData* self = mas->self;
	self->learning.store(true);
  self->level = mas->level;
  Version* c = koo::db->GetCurrentVersion();
  bool entered = false;

  if (!(self->Deleted())) {
  	bool filldata = false;
	  if (!(self->retraining.load())) {		// model learning
			self->mutex_delete_.Lock();
			self->string_keys = new std::vector<uint64_t>();
			uint64_t tmp_entry_num = 430185;
			if (koo::entry_size)
				tmp_entry_num = mas->meta->file_size / koo::entry_size;
			self->string_keys->reserve(tmp_entry_num);
			self->mutex_delete_.Unlock();

			filldata = self->FillData(c, mas->meta);
		} else filldata = true;			// model retraining

		if (filldata) {
#if TIME_W
			std::chrono::system_clock::time_point StartTime;
			if (self->level == 0) StartTime = std::chrono::system_clock::now();
#endif
#if RETRAIN
			if (self->Learn()) {
				entered = true;
#if TIME_W
				if (self->level == 0) {
					std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
					koo::learntime_l0 += nano.count();
					koo::num_learntime_l0++;
				}
#endif
			} //else fprintf(stderr, "\nLearning stopped\n\n");
			//std::cout << "[Learning Stopped] " << mas->meta->number << " (level: " << self->level << ")\n" << std::endl;
#else
		  self->Learn();
			entered = true;
#endif
#if AC_TEST
			koo::num_learned++;
#endif
#if AC_TEST3
			koo::num_files_learned_[self->level]++;
#endif
		}
  }

#if RETRAIN
	self->retraining.store(false);
#endif
  self->learning.store(false);
  koo::db->ReturnCurrentVersion(c);

#if DEBUG
  //if (fresh_write) {			// TODO
	  //self->WriteModel(koo::db->versions_->dbname_ + "/" + to_string(mas->meta->number) + ".fmodel");
		//self->num_entries_accumulated.array.clear();
  //}
#endif

	//lemma self->string_keys.clear();
	//lemma self->string_keys.shrink_to_fit();
	if (self->Deleted()) {
		self->string_segments.clear();
		self->string_segments.shrink_to_fit();
		self->mutex_delete_.Lock();
		if (self->string_keys) {
			delete self->string_keys;
			self->string_keys = nullptr;
		}
		self->mutex_delete_.Unlock();
	}
  //if (!fresh_write) delete mas->meta;
  delete mas->meta;
  delete mas;
	return entered ? 1 : 0;
}

// general model checker
bool LearnedIndexData::Learned() {
  if (learned_not_atomic)
    return true;
  else if (learned.load()) {
    learned_not_atomic = true;
    return true;
  } else
    return false;
}

bool LearnedIndexData::FillData(Version* version, FileMetaData* meta) {
  // if (filled) return true;

  if (version->FillData(koo::read_options, meta, this)) {
    // filled = true;
    if (Deleted()) return false;
    return true;
  }
  return false;
}

void LearnedIndexData::WriteModel(const string& filename) {
  //if (!learned.load()) return;
  if (Deleted() || !learned.load()) return;
#if MERGE
	std::ofstream ofs(filename, std::ios::binary);
	ofs.write(reinterpret_cast<const char*>(&koo::block_num_entries), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&koo::block_size), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&koo::entry_size), sizeof(uint64_t));
	size_t segs_size = string_segments.size();
	ofs.write(reinterpret_cast<const char*>(&segs_size), sizeof(size_t));
	for (Segment& s : string_segments) {
		ofs.write(reinterpret_cast<const char*>(&s.x), sizeof(uint64_t));
		ofs.write(reinterpret_cast<const char*>(&s.k), sizeof(double));
		ofs.write(reinterpret_cast<const char*>(&s.b), sizeof(double));
		ofs.write(reinterpret_cast<const char*>(&s.x_last), sizeof(uint64_t));
		ofs.write(reinterpret_cast<const char*>(&s.y_last), sizeof(uint32_t));
	}
	ofs.write(reinterpret_cast<const char*>(&min_key), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&max_key), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&size), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&level), sizeof(int));
#if MAX_MERGE_HISTORY
	ofs.write(reinterpret_cast<const char*>(&merge_history), sizeof(uint32_t));
#else
	ofs.write(reinterpret_cast<const char*>(&merged), sizeof(bool));
#endif
	ofs.write(reinterpret_cast<const char*>(&file_number), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&error), sizeof(double));

	for (size_t i=0; i<size; i++) {
		uint64_t key = (*string_keys)[i];
		ofs.write(reinterpret_cast<const char*>(&key), sizeof(uint64_t));
	}

	ofs.close();
#else
  std::ofstream output_file(filename);
  output_file.precision(15);
  output_file << koo::block_num_entries << " " << koo::block_size << " "
              << koo::entry_size << "\n";
  for (Segment& item : string_segments) {
    output_file << item.x << " " << item.k << " " << item.b << " " 
				<< item.x_last << " " << item.y_last << "\n";
  }
  output_file << "StartAcc"
              << " " << min_key << " " << max_key << " " << size << " " << level
              << " " << cost << "\n";
#endif
}

void LearnedIndexData::ReadModel(const string& filename, Version* v, FileMetaData* meta) {
  if (learned.load()) return;
#if MERGE
	std::ifstream ifs(filename, std::ios::binary);
	if (!ifs.good()) return;
	ifs.read(reinterpret_cast<char*>(&koo::block_num_entries), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&koo::block_size), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&koo::entry_size), sizeof(uint64_t));
	size_t segs_size;
	ifs.read(reinterpret_cast<char*>(&segs_size), sizeof(size_t));
	for (int i=0; i<segs_size; i++) {
		uint64_t x, x_last;
		double k, b;
		uint32_t y_last;
		ifs.read(reinterpret_cast<char*>(&x), sizeof(uint64_t));
		ifs.read(reinterpret_cast<char*>(&k), sizeof(double));
		ifs.read(reinterpret_cast<char*>(&b), sizeof(double));
		ifs.read(reinterpret_cast<char*>(&x_last), sizeof(uint64_t));
		ifs.read(reinterpret_cast<char*>(&y_last), sizeof(uint32_t));
		string_segments.emplace_back(Segment(x, k, b, x_last, y_last));
	}
	ifs.read(reinterpret_cast<char*>(&min_key), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&max_key), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&level), sizeof(int));
#if MAX_MERGE_HISTORY
	ifs.read(reinterpret_cast<char*>(&merge_history), sizeof(uint32_t));
#else
	ifs.read(reinterpret_cast<char*>(&merged), sizeof(bool));
#endif
	ifs.read(reinterpret_cast<char*>(&file_number), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&error), sizeof(double));

  string_keys = new std::vector<uint64_t>();
  string_keys->reserve(size+10);
  //bool ret = FillData(v, meta);
	for (int i=0; i<size; i++) {
		uint64_t key;
		ifs.read(reinterpret_cast<char*>(&key), sizeof(uint64_t));
		string_keys->emplace_back(key);
	}

	ifs.close();

#if DEBUG
	/*std::ofstream ofs("/koo/HyperLearningless/koo/data/segs_" + std::to_string(file_number) + "_read.txt");
	ofs << min_key << " " << max_key << " " << size << " " << level << " " << merge_history << "\n\n";
	ofs.precision(15);
	for (auto& s : string_segments) {
		ofs << "(" << s.x << ", )~(" << s.x_last << ", " << s.y_last << "): y = " << s.k << " * x + " << s.b << "\n";
	}
	ofs.close();*/
#endif
#else
  std::ifstream input_file(filename);

  if (!input_file.good()) return;
  input_file >> koo::block_num_entries >> koo::block_size >>
      koo::entry_size;
  while (true) {
    string x;
    double k, b;
    uint64_t x_last;
    uint32_t y_last;
    input_file >> x;
    if (x == "StartAcc") break;
    input_file >> k >> b >> x_last >> y_last;
    string_segments.emplace_back(atoll(x.c_str()), k, b, x_last, y_last);
  }
  input_file >> min_key >> max_key >> size >> level >> cost;
  while (true) {
    uint64_t first;
    string second;
    if (!(input_file >> first >> second)) break;
    //num_entries_accumulated.Add(first, std::move(second));
  }
#endif

  learned.store(true);
}

#if LEARN
LearnedIndexData::~LearnedIndexData() {
	//if (!buckets_data) delete buckets_data;
	//buckets_data = nullptr;
	// TODO unlink write했던 파일들 삭제
}

bool LearnedIndexData::Deleted() {
	if (deleted_not_atomic) return true;
	else if (deleted.load()) {
		deleted_not_atomic = true;
		return true;
	} else return false;
}

void LearnedIndexData::MarkDelete() {
	deleted.store(true);

	/*learned.store(false);			// TODO 둘까 말까
	learned_not_atomic = false;
	string_segments.clear();
	string_segments.shrink_to_fit();*/
	mutex_delete_.Lock();
	if (!learning.load() && string_keys) {
		delete string_keys;
		string_keys = nullptr;
		string_segments.clear();
		string_segments.shrink_to_fit();
	}
/*#if RETRAIN2 && AC_TEST
	if (extended_cnt) {
		std::ofstream ofs("/koo/HyperLearningless3/koo/data/retrain2.txt", std::ios::app);
		ofs << file_number << ": " << extended_cnt << ", " << actual_extended_cnt << std::endl;
		ofs.close();
	}
#endif*/
	mutex_delete_.Unlock();
	//string_keys->clear(); //lemma
	//string_keys->shrink_to_fit(); //lemma
}

void FileLearnedIndexData::DeleteModel(int number) {
	//leveldb::MutexLock l(&mutex);
	if (file_learned_index_data.size() <= number) return;
	if (file_learned_index_data[number] == nullptr) return;

	file_learned_index_data[number]->MarkDelete();
	//delete file_learned_index_data[number];
	//file_learned_index_data[number] = nullptr;
	return;
}
#endif

LearnedIndexData* FileLearnedIndexData::GetModel(int number) {
  if (file_learned_index_data.size() <= number) {
  	//mutex.Lock();
  	rw_lock_.LockWrite();
		if (file_learned_index_data.size() <= number) {
			file_learned_index_data.resize(number + 1000, nullptr);
			file_learned_index_data[number] = new LearnedIndexData((uint64_t)number);
			//mutex.Unlock();
			rw_lock_.UnlockWrite();
			return file_learned_index_data[number];
		}
		//mutex.Unlock();
		rw_lock_.UnlockWrite();
	}
  if (file_learned_index_data[number] == nullptr) {
  	//mutex.Lock();
  	rw_lock_.LockWrite();
		if (file_learned_index_data[number] == nullptr) {			// TODO compaction후 merged model들 한번에 만들기?
			file_learned_index_data[number] = new LearnedIndexData((uint64_t)number);
			//mutex.Unlock();
			rw_lock_.UnlockWrite();
			return file_learned_index_data[number];
		}
		//mutex.Unlock();
		rw_lock_.UnlockWrite();
	}
  return file_learned_index_data[number];
}

#if LEARN
LearnedIndexData* FileLearnedIndexData::GetModelImm(int number) {
  return file_learned_index_data[number];
}

LearnedIndexData* FileLearnedIndexData::GetModelForLookup(int number) {
	rw_lock_.LockRead();
	if (file_learned_index_data.size() <= number) {
		rw_lock_.UnlockRead();
		return nullptr;
	}
	if (file_learned_index_data[number] == nullptr) {
		rw_lock_.UnlockRead();
		return nullptr;
	}
	rw_lock_.UnlockRead();
  return file_learned_index_data[number];
}
#endif

bool FileLearnedIndexData::FillData(Version* version, FileMetaData* meta) {
  LearnedIndexData* model = GetModel(meta->number);
  return model->FillData(version, meta);
}

//std::vector<std::string>& FileLearnedIndexData::GetData(FileMetaData* meta) {
std::vector<uint64_t>& FileLearnedIndexData::GetData(FileMetaData* meta) {
//std::vector<Slice>& FileLearnedIndexData::GetData(FileMetaData* meta) {
  auto* model = GetModel(meta->number);
  return *(model->string_keys);
}

std::pair<uint64_t, uint64_t> FileLearnedIndexData::GetPosition(
    Slice& key, int file_num) {
    //const Slice& key, int file_num) {
  return file_learned_index_data[file_num]->GetPosition(key);
}

FileLearnedIndexData::~FileLearnedIndexData() {
	//leveldb::MutexLock l(&mutex);
	rw_lock_.LockWrite();
  for (auto pointer : file_learned_index_data) {
    if (pointer != nullptr) delete pointer;
  }
	//rw_lock_.UnlockWrite();
}

#endif

}  // namespace koo
