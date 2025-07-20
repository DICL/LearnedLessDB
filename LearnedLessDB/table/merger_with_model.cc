// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger_with_model.h"

#include <algorithm>

#include "hyperleveldb/comparator.h"
#include "hyperleveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "db/version_set.h"
#include "koo/learned_index.h"
#include "koo/merge.h"

namespace leveldb {

namespace {

struct preSegment {
  uint64_t start;
  uint64_t end;
  uint32_t size0;
  uint32_t size1;
  double k;
  double b;

  preSegment() : start(0), end(0), size0(0), size1(0), k(0), b(0) {}
  preSegment(uint64_t _start, uint64_t _end, uint32_t _size0, uint32_t _size1) : start(_start), end(_end), size0(_size0), size1(_size1) {}

  preSegment& operator+=(const preSegment& other) {
    if (start == 0)
      this->start = other.start;
    this->end = other.end;
    this->size0 += other.size0;
    this->size1 += other.size1;
    return *this;
  }
  uint32_t size() const { return size0 + size1; }

  void complete() {
    if (end) {
      if (end != start) {
        k = static_cast<double>(size()) / static_cast<double>(end - start);
        b = static_cast<double>(start) * k * -1.0;
      }
    }
  }
};

class IntKeyIter {
 private:
  int off_ = 0;
  int file_off_ = 0;
  int file_size_ = 0;
  Compaction* c_;
  const int level_;
  std::vector<uint64_t> *string_keys_;
  bool valid_ = true;
  size_t num_input_files_;

 public:
  IntKeyIter(Compaction* c, int level) : 
            c_(c), level_(level),
            string_keys_(koo::file_data->GetModelForLookup(c_->input(level_, file_off_)->number)->string_keys) {
    num_input_files_ = c_->num_input_files(level_);
    if (string_keys_ != nullptr) {
      file_size_ = string_keys_->size();
    } else {
      fprintf(stderr, "GetModel nullptr %u\n", c_->level() + level_);
      valid_ = false;
    }
  }

  void Next() {
    off_++;
    if (off_ >= file_size_) {
      file_off_++;
      if (file_off_ == c_->num_input_files(level_)) {
        valid_ = false;
        return;
      }
      string_keys_ = koo::file_data->GetModelForLookup(c_->input(level_, file_off_)->number)->string_keys;
      if (string_keys_ != nullptr) {
        file_size_ = string_keys_->size();
      } else {
        fprintf(stderr, "GetModel nullptr 22 %u\n", c_->level() + level_);
        valid_ = false;
      }
      off_ = 0;
    }
  }
  uint64_t key() {
    return (*string_keys_)[off_];
  }
  bool Valid() {
    return valid_;
  }
};

class ModelIter {
 private:
  int off_ = 0;
  int file_off_ = 0;
  Compaction* c_;
  const int level_;
  koo::LearnedIndexData* m_;
  bool is_f_ = true;
  bool valid_ = true;
  koo::Segment curr_;

  inline double get_pos(uint64_t s) { return curr_.k * static_cast<double>(s) + curr_.b; }
 public:
  ModelIter(Compaction* c, int level) : 
            c_(c), level_(level),
            m_(koo::file_data->GetModelForLookup(c_->input(level_, file_off_)->number)),
            curr_(m_->GetSegment(off_)) {
  }

  void DeleteStringSegments() {
  	if (m_->is_retrained_) {
	  	m_->string_segments.clear();
			m_->string_segments.shrink_to_fit();
		}
	}

  void Next() {
    if (is_f_) {
      is_f_ = false;
    }
    else {
      is_f_ = true;
      off_++;
      if (off_ == m_->GetSegmentSize() - 1) {
        file_off_++;
        if (file_off_ == c_->num_input_files(level_)) {
          valid_ = false;
          return;
        }
        m_ = koo::file_data->GetModelForLookup(c_->input(level_, file_off_)->number);
        off_ = 0;
      }
      curr_ = m_->GetSegment(off_);
    }
  }
  uint64_t key() {
    uint64_t result;
    if (is_f_)
      result = curr_.x;
    else
      result = curr_.x_last;
    return result;
  }
  bool Valid() {
    return valid_;
  }
  uint32_t GetSize(uint64_t s, uint64_t e) {
    if (is_f_)
      return 0;
    else {
    	double tmp = get_pos(e) - get_pos(s);
    	if (tmp <= 0 /*|| std::isnan(tmp)*/) return 0;
    	return static_cast<uint32_t>(tmp);
    }
  }
};

class MergeModelIter {
 private:
  Compaction* c_;
  ModelIter* child_[2];
  uint64_t last_key_;
  int next_ = -1;
  bool valid_ = true;
 public:
  MergeModelIter(Compaction* c) : c_(c) {
    for(int i = 0; i < 2; i++)
      child_[i] = new ModelIter(c_, i);
    last_key_ = key();
  }

  preSegment GetNext() {
    preSegment result;
    if (valid_) {
      while(result.size() < 10) {
        Next();
        uint64_t next_key = key();
        if (next_key == 0) {
          valid_ = false;
          break;
        }
        preSegment next_pre(last_key_, next_key, child_[0]->GetSize(last_key_, next_key), child_[1]->GetSize(last_key_, next_key));
        last_key_ = next_key;
        result += next_pre;
      }
    }
    result.complete();
    return result;
  }

  void DeleteStringSegments() {
  	for (int i=0; i<2; i++)
  		child_[i]->DeleteStringSegments();
	}

 private:
  uint64_t key() {
    if (child_[0]->Valid()) {
      uint64_t key0 = child_[0]->key();
      if (child_[1]->Valid()) {
        uint64_t key1 = child_[1]->key();
        if (key0 > key1) {
          next_ = 1;
          return key1;
        } else if (key0 < key1) {
          next_ = 0;
          return key0;
        } else {
          next_ = 2;
          return key0;
        }
      } else {
        next_ = 0;
        return key0;
      }
    } else {
      if (child_[1]->Valid()) {
        uint64_t key1 = child_[1]->key();
        next_ = 1;
        return key1;
      } else {
        next_ = -1;
        return 0;
      }
    }
  }

  void Next() {
    if (next_ == 0 || next_ == 1)
      child_[next_]->Next();
    if (next_ == 2) {
      child_[0]->Next();
      child_[1]->Next();
    }
  }

};

class MergingWithModelIterator : public Iterator {
 public:
  MergingWithModelIterator(const Comparator* comparator, Iterator** children, Compaction* c)
      : comparator_(comparator),
        children_(new IteratorWrapper[2]),
        comp_(c), m_iter_(c), max_entry_size_(MaxFileSizeForLevel(c->level() + 1) / koo::entry_size) {
    children_[0].Set(children[0]);
    children_[1].Set(children[1]);

    children_[0].SeekToFirst();
    children_[1].SeekToFirst();
    last_key_ = 0;
    current_key_ = 0;
    for(int i = 0; i < 2; i++)
      int_key_iter_[i] = new IntKeyIter(c, i);
  }

  virtual ~MergingWithModelIterator() {
  	m_iter_.DeleteStringSegments();
    delete[] children_;
    if (top_buffer_)
      free(top_buffer_);
    if (string_keys_) {
      if(string_keys_->size()) {
      } else {
        delete string_keys_;
        string_keys_ = nullptr;
      }
    }
  }

  virtual bool Valid() const {
    return valid_;
  }

  virtual void SeekToFirst() {
    MakeBuffer();
    merge_ = new koo::MergeModel();
    string_keys_ = new std::vector<uint64_t>();
    string_keys_->reserve(max_entry_size_);
    PrepareNext();
    curr_seg_count_ = 1;
  }

  virtual void SeekToLast() {
    fprintf(stderr, "Merger With Model doesn't support SeekToLast()");
  }

  virtual void Seek(const Slice& target) {
    fprintf(stderr, "Merger With Model doesn't support Seek()");
  }

  virtual void Next() {
    if (!int_key_skip_)
      string_keys_->push_back(int_key());
    else
      int_key_skip_ = false;
    while(PrepareNext())
      MakeBuffer();
    curr_seg_count_++;
  }

  virtual void Prev() {
    fprintf(stderr, "Merger With Model doesn't support Prev()");
  }

  virtual Slice key() const {
    assert(Valid());
    if (is_l0_)
      return key_buffer_[top_buffer_[index_].index];
    else
      return children_[1].key();
  }

  uint64_t int_key() const {
    assert(Valid());
    if (is_l0_)
      return top_buffer_[index_].int_key;
    else
      return int_key_iter_[1]->key();
  }

  virtual Slice value() const {
    assert(Valid());
    if (is_l0_)
      return value_buffer_[top_buffer_[index_].index];
    else
      return children_[1].value();
  }

  virtual const Status& status() const {
    // XXX this value can easily be cached
    for (int i = 0; i < 2; i++) {
      if (!children_[i].status().ok()) {
        return children_[i].status();
      }
    }
    return status_;
  }

 //private:
  MergingWithModelIterator(const MergingWithModelIterator&);
  MergingWithModelIterator& operator = (const MergingWithModelIterator&);

  struct BufStruct {
    uint64_t int_key = 0;
    int index = 0;
    BufStruct() = default;
    BufStruct(uint64_t k, int i) : int_key(k), index(i) {}
  };

  const Comparator* comparator_;
  IteratorWrapper* children_;
  Status status_;

  Compaction* comp_;

  BufStruct* top_buffer_ = nullptr;
  std::vector<std::string> key_buffer_;
  std::vector<std::string> value_buffer_;
  MergeModelIter m_iter_;
  bool valid_ = true;
  uint64_t top_buffer_size_ = 0;

  uint64_t index_;
  uint64_t y_inf_;
  uint64_t key_int_;
  uint64_t final_y_;
  bool is_l0_;
  int move_which_;
  preSegment ps;
  bool is_l1_end;

  koo::MergeModel* merge_ = nullptr;
  uint64_t last_key_;
  uint64_t current_key_;
  std::vector<uint64_t> *string_keys_ = nullptr;

  size_t curr_seg_count_;

  IntKeyIter* int_key_iter_[2];
  bool int_key_skip_ = false;

  const uint64_t max_entry_size_;

  int PrepareNext() {
    last_key_ = current_key_;
    if (!valid_)
      return 0;
    if (move_which_ == 1 || move_which_ == 3)
      index_++;
    if (move_which_ == 2 || move_which_ == 3) {
      children_[1].Next();
      int_key_iter_[1]->Next();
      if (children_[1].Valid()) {
        key_int_ = int_key_iter_[1]->key();
        y_inf_ = std::max(index_, get_buffer_pos(key_int_, ps));
        if (key_int_ > ps.end) {
          y_inf_ = UINT64_MAX;
          is_l1_end = true;
        }
      } else {
        y_inf_ = UINT64_MAX;
        is_l1_end = true;
      }
    }

    while(index_ < y_inf_) {
      if (top_buffer_[index_].int_key) {
        is_l0_ = true;
        move_which_ = 1;
        current_key_ = top_buffer_[index_].int_key;
        return 0;
      }
      index_++;
      if(is_l1_end && index_ > final_y_) {
        return 1;
      }
    }
    if (top_buffer_[index_].int_key) {
      if (top_buffer_[index_].int_key < key_int_) {
        current_key_ = top_buffer_[index_].int_key;
        is_l0_ = true;
        move_which_ = 1;
      } else if (top_buffer_[index_].int_key == key_int_) {
        current_key_ = top_buffer_[index_].int_key;
        is_l0_ = true;
        move_which_ = 3;
      } else {
        current_key_ = key_int_;
        is_l0_ = false;
        move_which_ = 2;
      }
    } else {
      current_key_ = key_int_;
      is_l0_ = false;
      move_which_ = 2;
    }
    return 0;
  }

  void MakeBuffer() {
    if(merge_) {
      if (curr_seg_count_ != 0)
        merge_->seg_infos.push_back(std::make_pair(ps.end, curr_seg_count_));
    }
    ps = m_iter_.GetNext();
    curr_seg_count_ = 0;
    if (ps.end == 0) {
      valid_ = false;
      return;
    }

    if (top_buffer_size_ < ps.size() * 2 + 100) {
      if (top_buffer_)
        free(top_buffer_);
      top_buffer_ = (BufStruct*)calloc(ps.size() * 2 + 100, sizeof(BufStruct));
      top_buffer_size_ = ps.size() * 2 + 100;
    } else {
      std::memset(top_buffer_, 0, (final_y_+1)*sizeof(BufStruct));
    }

    key_buffer_.clear();
    value_buffer_.clear();
    key_buffer_.reserve(ps.size0+20);
    value_buffer_.reserve(ps.size0+20);
    int index_for_0 = 0;
    final_y_ = 0;
    while(children_[0].Valid()) {
      uint64_t key_int = int_key_iter_[0]->key();
      if (key_int > ps.end) 
        break;
      uint64_t y_inf = get_buffer_pos(key_int, ps);
      while (top_buffer_[y_inf].int_key) {
        y_inf++;
      }
      top_buffer_[y_inf].int_key = key_int;
      top_buffer_[y_inf].index = index_for_0++;
      final_y_ = y_inf;
      key_buffer_.push_back(children_[0].key().ToString());
      value_buffer_.push_back(children_[0].value().ToString());
      children_[0].Next();
      int_key_iter_[0]->Next();
    }
    index_ = 0;
    if(children_[1].Valid()) {
      key_int_ = int_key_iter_[1]->key();
      y_inf_ = get_buffer_pos(key_int_, ps);
      if (key_int_ > ps.end) {
        y_inf_ = UINT64_MAX;
        is_l1_end = true;
      } else {
        is_l1_end = false;
      }
    }
    move_which_ = 0;
  }

  uint64_t get_buffer_pos(uint64_t user_key, preSegment seg) {
    double result = user_key * seg.k + seg.b;
    return result < 0 ? 0 : static_cast<uint64_t>(std::floor(result));
  }

 public:
  std::pair<void*, std::vector<uint64_t>*> GetMergeModel(bool include_current) {
    if (include_current) {
      if (curr_seg_count_ > 0) {
        merge_->seg_infos.push_back(std::make_pair(current_key_, curr_seg_count_));
      }
      string_keys_->push_back(current_key_);
      curr_seg_count_ = 0;
      int_key_skip_ = true;
    } else {
      if (curr_seg_count_ > 1) {
        merge_->seg_infos.push_back(std::make_pair(last_key_, curr_seg_count_-1));
      } else if (curr_seg_count_ < 1) {
        fprintf(stderr, "GetMergeModel something wrong!!!!!\n");
      }
      curr_seg_count_ = 1;
    }
    koo::MergeModel* result = merge_;
    std::vector<uint64_t> *string_keys = string_keys_;
    merge_ = new koo::MergeModel();
    string_keys_ = new std::vector<uint64_t>();
    string_keys_->reserve(max_entry_size_);
    return std::make_pair((void*)result, string_keys);
  }
  
};

}  // namespace 

Iterator* NewMergingWithModelIterator(const Comparator* cmp, Iterator** list, Compaction* c) {
  return new MergingWithModelIterator(cmp, list, c);
}

void PrintIterStats(Iterator* iter) { 
  MergingWithModelIterator* input_tmp = (MergingWithModelIterator*)iter;
  fprintf(stderr, "PrintIterStats %d %lu %lu %lu %u %u %lu\n",
                    input_tmp->is_l0_, input_tmp->index_,
                    input_tmp->ps.start, input_tmp->ps.end,
                    input_tmp->ps.size0, input_tmp->ps.size1, input_tmp->final_y_);
}
std::pair<void*, std::vector<uint64_t>*> ReturnMergeModel(bool include_current, Iterator* iter) {
  return ((MergingWithModelIterator*)iter)->GetMergeModel(include_current);
}
}  // namespace leveldb
