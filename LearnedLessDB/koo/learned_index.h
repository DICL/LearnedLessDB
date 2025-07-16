//
// Created by daiyi on 2020/02/02.
//

#ifndef LEVELDB_LEARNED_INDEX_H
#define LEVELDB_LEARNED_INDEX_H


#include <vector>
#include <cstring>
#include "koo/util.h"
#include "koo/plr.h"
#include "koo/koo.h"
#include "koo/extenthash.h"
#include "port/port.h"


using std::string;
using leveldb::Slice;
using leveldb::Version;
using leveldb::FileMetaData;

#if EXTENT_HASH
struct Extent;
#endif

namespace leveldb { class FileMetaData; }

namespace koo {

#if LEARN

    class LearnedIndexData;

    // An array collecting the total number of keys in a level in or before each file. One per level.
    // Used to get the target file when a level model produces the predicted position in the level. 
    /*class AccumulatedNumEntriesArray {
        friend class LearnedIndexData;

    public:
        std::vector<std::pair<uint64_t, string>> array;
    public:
        AccumulatedNumEntriesArray() = default;
        // During learning, add info to this array with the number of entries in a file and its largest key
        void Add(uint64_t num_entries, string&& key);
        // Given a predicted interval, return the target file index in param:index.
        bool Search(const Slice& key, uint64_t lower, uint64_t upper, size_t* index, uint64_t* relative_lower, uint64_t* relative_upper);
        // Used for testing assuming the model has no error
        bool SearchNoError(uint64_t position, size_t* index, uint64_t* relative_position);
        uint64_t NumEntries() const;
    };*/


    class VersionAndSelf {
    public:
        Version* version;
        int v_count;
        LearnedIndexData* self;
        int level;
    };

    class MetaAndSelf {
    public:
        Version* version;
        int v_count;
        FileMetaData* meta;
        LearnedIndexData* self;
        int level;
    };

    // The structure for learned index. Could be a file model or a level model
    class LearnedIndexData {
        friend class leveldb::Version;
        friend class leveldb::VersionSet;
    private:
        // predefined model error
        double error;
        // some flags used in online learning to control the state of the model
        std::atomic<bool> learned;
        std::atomic<bool> aborted;
        bool learned_not_atomic;
        std::atomic<bool> learning;
#if LEARN
				bool deleted_not_atomic;
				std::atomic<bool> deleted;
#if MODEL_COMPACTION
				port::Mutex mutex_delete_;
#endif
#endif
#if MERGE
#if MAX_MERGE_HISTORY
        uint32_t merge_history;			// 0: learned, 1~: merged
#else
				bool merged;
#endif
#if RETRAIN
				std::atomic<bool> retraining;
#endif
#endif
#if EXTENT_HASH
				struct Bucket {
					Bucket() : empty(true) {}

					bool empty;
					uint64_t key_x;
					uint64_t key_x_last;
					uint32_t seg_num;
				};

				Bucket buckets[BUCKET_LEN];
				std::atomic<bool> hashed;
				bool hashed_not_atomic;
#endif
    public:
#if LEARN
				uint64_t file_number;
#endif
#if RETRAIN2
				bool error_extented = false;
#if AC_TEST
				//std::atomic<uint32_t> extended_cnt = 0;
				//std::atomic<uint32_t> actual_extended_cnt = 0;
#endif
#endif
        // is the data of this model filled (ready for learning)
        bool filled;

        // Learned linear segments and some other data needed
        std::vector<Segment> string_segments;
        uint64_t min_key;
        uint64_t max_key;
        uint64_t size;			// SST내 entry 개수

    public:
#if RETRAIN || MODEL_COMPACTION
        bool is_retrained_ = false;
        std::vector<Segment> string_segments_bak;
#endif
#if MODEL_COMPACTION
        //bool is_replaced_ = false;
        //SpinLock string_segments_lock_;
        Segment GetSegment(int offset) {
            //std::unique_lock<SpinLock> lock(string_segments_lock_);
            /*if (is_replaced_)
                return string_segments_bak[offset];
            else*/
                return string_segments[offset];
        }
        size_t GetSegmentSize() {
            //std::unique_lock<SpinLock> lock(string_segments_lock_);
            /*if (is_replaced_)
                return string_segments_bak.size();
            else*/
                return string_segments.size();
        }
        // all keys in the file/level to be leraned from
        std::vector<uint64_t> *string_keys = nullptr;
#else
        std::vector<uint64_t> string_keys;
#endif
        // only used in level models
        //AccumulatedNumEntriesArray num_entries_accumulated;

        int level;
        uint64_t cost;

//        int num_neg_model = 0, num_pos_model = 0, num_neg_baseline = 0, num_pos_baseline = 0;
//        uint64_t time_neg_model = 0, time_pos_model = 0, time_neg_baseline = 0, time_pos_baseline = 0;
//
//        int num_neg_model_p = 0, num_pos_model_p = 0, num_neg_baseline_p = 0, num_pos_baseline_p = 0, num_files_p = 0;
//        uint64_t time_neg_model_p = 0, time_pos_model_p = 0, time_neg_baseline_p = 0, time_pos_baseline_p = 0;
//        double gain_p = 0;
//        uint64_t file_size = 0;



#if MERGE
#if RETRAIN
#if EXTENT_HASH
#if MAX_MERGE_HISTORY
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						retraining(false),
						merge_history(0),
						hashed(false), hashed_not_atomic(false) {};
#else			// MAX_MERGE_HISTORY
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						retraining(false),
						merged(false),
						hashed(false), hashed_not_atomic(false) {};
#endif		// MAX_MERGE_HISTORY
#else				// EXTENT_HASH
#if MAX_MERGE_HISTORY
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						retraining(false),
						merge_history(0) {};
#else			// MAX_MERGE_HISTORY
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						retraining(false),
						merged(false) {};
#endif		// MAX_MERGE_HISTORY
#endif			// EXTENT_HASH
#else					// RETRAIN
#if MAX_MERGE_HISTORY
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						merge_history(0) {};
#else
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						merged(false) {};
#endif		// MAX_MERGE_HISTORY
#endif				// RETRAIN
#else						// MERGE
        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0) {};
#endif					// MERGE

        LearnedIndexData(const LearnedIndexData& other) = delete;
#if LEARN
        ~LearnedIndexData();
        bool Deleted();
        void MarkDelete();
#endif

        // Inference function. Return the predicted interval.
        // If the key is in the training set, the output interval guarantees to include the key
        // otherwise, the output is undefined!
        // If the output lower bound is larger than MaxPosition(), the target key is not in the file
#if MERGE
        std::pair<uint64_t, uint64_t> GetPosition(Slice& key);
#else
        std::pair<uint64_t, uint64_t> GetPosition(const Slice& key) const;
#endif
        uint64_t MaxPosition() const;
        double GetError() const;
#if RETRAIN2
        void SetError(double cur_error, uint64_t extra_error);
#endif

#if MERGE
#if MAX_MERGE_HISTORY
				void SetMergedModel(std::vector<Segment>& segs, uint32_t history);
				bool CheckMergeHistory();
				uint32_t GetMergeHistory();
#else
				void SetMergedModel(std::vector<Segment>& segs);
				bool Merged();
#endif
#if RETRAIN
				//bool InitModelForRetraining();
				//bool Retraining();
				void FreezeModel();
				void UnfreezeModel();
				bool SetRetraining();
#endif
#endif
#if EXTENT_HASH
				void InitBuckets();
				bool InsertExtentImpl(uint64_t bucket_key, Extent& e, uint32_t value);
				bool InsertExtent(Extent e, uint32_t value);
				bool GetExtent(uint64_t key, uint32_t& v);
				bool Hashed();
#endif
        
        // Learning function and checker (check if this model is available)
        bool Learn();
        bool Learned();
        static uint64_t FileLearn(void* arg);

        // Load all the keys in the file/level
        bool FillData(Version* version, FileMetaData* meta);

        // writing this model to disk and load this model from disk
        void WriteModel(const string& filename);
        void ReadModel(const string& filename, Version* v, FileMetaData* meta);

        bool Learn(bool file);
    };

    // an array storing all file models and provide similar access interface with multithread protection
    class FileLearnedIndexData {
    private:
        //leveldb::port::Mutex mutex;
        koo::RWLock rw_lock_;
        std::vector<LearnedIndexData*> file_learned_index_data;
    public:
				//uint64_t watermark;

        bool FillData(Version* version, FileMetaData* meta);
        //std::vector<std::string>& GetData(FileMetaData* meta);
        //std::vector<uint64_t>& GetData(FileMetaData* meta);
        //std::vector<Slice>& GetData(FileMetaData* meta);
        std::pair<uint64_t, uint64_t> GetPosition(Slice& key, int file_num);
        //std::pair<uint64_t, uint64_t> GetPosition(const Slice& key, int file_num);
        LearnedIndexData* GetModel(int number);
#if LEARN
        LearnedIndexData* GetModelImm(int number);
        LearnedIndexData* GetModelForLookup(int number);
				void DeleteModel(int number);
#endif
        ~FileLearnedIndexData();
    };

    class LevelLearnedIndexData {
     private:
      leveldb::port::Mutex mutex;
      std::vector<LearnedIndexData*> level_learned_index_data;
     public:

    };

#endif

}

#endif //LEVELDB_LEARNED_INDEX_H
