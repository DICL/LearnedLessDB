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

namespace koo {

std::pair<uint64_t, uint64_t> LearnedIndexData::GetPosition(
#if MERGE
    Slice& target_x) {
#else
    const Slice& target_x) const {
#endif
#if RETRAIN
  assert(string_segments.size() > 1 || string_segments_bak.size() > 1);
#else
  assert(string_segments.size() > 1);
#endif

  // check if the key is within the model bounds
  uint64_t target_int = target_x.SliceToInteger();
  if (target_int > max_key) return std::make_pair(size, size);
  if (target_int < min_key) return std::make_pair(size, size);

  // binary search between segments
  double k, b;
  if (is_retrained_) {
	  uint32_t left = 0, right = (uint32_t)string_segments_bak.size() - 1;
		while (left != right - 1) {
			uint32_t mid = (right + left) / 2;
	    if (target_int < string_segments_bak[mid].x) right = mid;
		  else left = mid;
		}
		k = string_segments_bak[left].k;
		b = string_segments_bak[left].b;
	} else {
	  uint32_t left = 0, right = (uint32_t)string_segments.size() - 1;
		while (left != right - 1) {
			uint32_t mid = (right + left) / 2;
	    if (target_int < string_segments[mid].x) right = mid;
		  else left = mid;
		}
		k = string_segments[left].k;
		b = string_segments[left].b;
  }
  
	/*if (!(string_segments[left].x <= target_int && target_int <= string_segments[left].x_last))
		return std::make_pair(size, size);*/

  // calculate the interval according to the selected segment
#if NORMARLIZE_KEY
	uint64_t nor_key = target_int - min_key + 1;
  double result = static_cast<double>(nor_key) * k + b;
#else
  double result = target_int * k + b;
#endif
#if RETRAIN2
	double error = GetError();
#endif
  uint64_t lower =
      result - error > 0 ? (uint64_t)std::floor(result - error) : 0;
  uint64_t upper = (uint64_t)std::ceil(result + error);
	upper = upper < size ? upper : size - 1;
#if MERGE
	if (lower >= size) {
		if (merged) lower = upper - 2*error - 1;
		else return std::make_pair(size, size);
	}
#endif

  return std::make_pair(lower, upper);
}

uint64_t LearnedIndexData::MaxPosition() const { return size - 1; }

double LearnedIndexData::GetError() const { return error; }

#if RETRAIN2
void LearnedIndexData::SetError(double cur_error, uint64_t extra_error) { 
	//if (cur_error == error || cur_error + extra_error > error) {
	if (cur_error + extra_error > error) {
		error = cur_error + extra_error;
	}
}
#endif

#if MERGE
void LearnedIndexData::SetMergedModel(std::vector<Segment>& segs) {
	error = koo::merge_model_error;
	merged = true;
	string_segments = std::move(segs);
	learned.store(true);
	return;
}

bool LearnedIndexData::Merged() {
	return merged;
}

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
	if (retraining.load()) return false;
	bool try_retraining = false;
	if (!retraining.compare_exchange_weak(try_retraining, !try_retraining)) return false;
	return true;
}
#endif
#endif

// Actual function doing learning
bool LearnedIndexData::Learn() {
  // FILL IN GAMMA (error)
  PLR plr = PLR(koo::learn_model_error);

  // check if data if filled
#if MODEL_COMPACTION
	if (string_keys->empty()) {
		string_keys->shrink_to_fit();
#else
	if (string_keys.empty()) {
		string_keys.shrink_to_fit();
#endif
		return false;
	}

  // fill in some bounds for the model
#if MODEL_COMPACTION
	min_key = string_keys->front();
	max_key = string_keys->back();
  size = string_keys->size();
#else
	min_key = string_keys.front();
	max_key = string_keys.back();
  size = string_keys.size();
#endif
#if TIME_W
	koo::learn_size += size;
	koo::num_learn_size++;
#endif

  // actual training
#if MODEL_COMPACTION
  std::vector<Segment> segs = plr.train(*string_keys);
#else
  std::vector<Segment> segs = plr.train(string_keys);
#endif
  if (segs.empty()) return false;
  if (Deleted()) return false;
#if MERGE
	if (segs.front().x != min_key) segs.front().x = min_key;
	if (segs.back().x_last != max_key) segs.back().x_last = max_key;
#endif
  // fill in a dummy last segment (used in segment binary search)
  segs.push_back((Segment){max_key, 0, 0, 0, 0});

#if RETRAIN
	if (retraining.load()) {
		string_segments_bak = std::move(segs);
		error = koo::learn_model_error;
		is_retrained_ = true;
		merged = false;
#if AC_TEST
		koo::num_retrained++;
#endif
	} else {
		string_segments = std::move(segs);
	}
#else		// RETRAIN
  string_segments = std::move(segs);
#endif
	learned.store(true);
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
#if MODEL_COMPACTION
#if RETRAIN
	  if (!(self->retraining.load())) {		// model learning
#endif
			self->mutex_delete_.Lock();
			self->string_keys = new std::vector<uint64_t>();
			uint64_t tmp_entry_num = 430185;
			if (koo::entry_size)
				tmp_entry_num = mas->meta->file_size / koo::entry_size;
			self->string_keys->reserve(tmp_entry_num);
			self->mutex_delete_.Unlock();

			filldata = self->FillData(c, mas->meta);
#if RETRAIN
		} else filldata = true;			// model retraining
#endif
#else
		filldata = self->FillData(c, mas->meta);
#endif

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
				koo::learn_bytesize += mas->meta->file_size;
				koo::num_learn_bytesize++;
#endif
			} //else fprintf(stderr, "\nLearning stopped\n\n");
#else
		  if (self->Learn()) {
				entered = true;
			}
#endif
#if AC_TEST
			koo::num_learned++;
#endif
#if MODEL_BREAKDOWN
			koo::lm_num[self->level]++;
			koo::lm_keys[self->level] += self->size;
			koo::lm_segs[self->level] += self->string_segments.size() - 1;
#endif
		}
  }

  self->learning.store(false);
  koo::db->ReturnCurrentVersion(c);

#if !MODEL_COMPACTION
	self->string_keys.clear();
	self->string_keys.shrink_to_fit();
#endif
	if (self->Deleted()) {
		self->string_segments.clear();
		self->string_segments.shrink_to_fit();
		if (self->is_retrained_) {
			self->string_segments_bak.clear();
			self->string_segments_bak.shrink_to_fit();
		}
#if MODEL_COMPACTION
		self->mutex_delete_.Lock();
		if (self->string_keys) {
			delete self->string_keys;
			self->string_keys = nullptr;
		}
		self->mutex_delete_.Unlock();
#endif
	}
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
  if (version->FillData(koo::read_options, meta, this)) {
    if (Deleted()) return false;
    return true;
  }
  return false;
}

void LearnedIndexData::WriteModel(const string& filename) {
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
	ofs.write(reinterpret_cast<const char*>(&merged), sizeof(bool));
	ofs.write(reinterpret_cast<const char*>(&file_number), sizeof(uint64_t));
	ofs.write(reinterpret_cast<const char*>(&error), sizeof(double));

#if MODEL_COMPACTION
	for (size_t i=0; i<size; i++) {
		uint64_t key = (*string_keys)[i];
		ofs.write(reinterpret_cast<const char*>(&key), sizeof(uint64_t));
	}
#endif

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

#if MODEL_BREAKDOWN
	if (merged) koo::num_mm[level]++;
	else koo::num_lm[level]++;
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
	ifs.read(reinterpret_cast<char*>(&merged), sizeof(bool));
	ifs.read(reinterpret_cast<char*>(&file_number), sizeof(uint64_t));
	ifs.read(reinterpret_cast<char*>(&error), sizeof(double));

#if MODEL_COMPACTION
  string_keys = new std::vector<uint64_t>();
  string_keys->reserve(size+10);
	for (int i=0; i<size; i++) {
		uint64_t key;
		ifs.read(reinterpret_cast<char*>(&key), sizeof(uint64_t));
		string_keys->emplace_back(key);
	}
#endif

	ifs.close();

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

LearnedIndexData::~LearnedIndexData() {
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

#if MODEL_COMPACTION
	mutex_delete_.Lock();
	if (!learning.load() && string_keys) {
		delete string_keys;
		string_keys = nullptr;
		if (is_retrained_) {
			string_segments_bak.clear();
			string_segments_bak.shrink_to_fit();
		}
		string_segments.clear();
		string_segments.shrink_to_fit();
	}
	mutex_delete_.Unlock();
#else
	if (is_retrained_) {
		string_segments_bak.clear();
		string_segments_bak.shrink_to_fit();
	}
	string_segments.clear();
	string_segments.shrink_to_fit();
#endif
}

void FileLearnedIndexData::DeleteModel(int number) {
	if (file_learned_index_data.size() <= number) return;
	if (file_learned_index_data[number] == nullptr) return;

	file_learned_index_data[number]->MarkDelete();
	return;
}

LearnedIndexData* FileLearnedIndexData::GetModel(int number) {
  if (file_learned_index_data.size() <= number) {
  	rw_lock_.LockWrite();
		if (file_learned_index_data.size() <= number) {
			file_learned_index_data.resize(number + 1000, nullptr);
			file_learned_index_data[number] = new LearnedIndexData((uint64_t)number);
			rw_lock_.UnlockWrite();
			return file_learned_index_data[number];
		}
		rw_lock_.UnlockWrite();
	}
  if (file_learned_index_data[number] == nullptr) {
  	rw_lock_.LockWrite();
		if (file_learned_index_data[number] == nullptr) {
			file_learned_index_data[number] = new LearnedIndexData((uint64_t)number);
			rw_lock_.UnlockWrite();
			return file_learned_index_data[number];
		}
		rw_lock_.UnlockWrite();
	}
  return file_learned_index_data[number];
}

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

bool FileLearnedIndexData::FillData(Version* version, FileMetaData* meta) {
  LearnedIndexData* model = GetModel(meta->number);
  return model->FillData(version, meta);
}

std::pair<uint64_t, uint64_t> FileLearnedIndexData::GetPosition(
    Slice& key, int file_num) {
  return file_learned_index_data[file_num]->GetPosition(key);
}

FileLearnedIndexData::~FileLearnedIndexData() {
	rw_lock_.LockWrite();
  for (auto pointer : file_learned_index_data) {
    if (pointer != nullptr) delete pointer;
  }
}


}  // namespace koo
