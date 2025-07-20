//
// Created by daiyi on 2020/02/02.
//

#ifndef LEVELDB_LEARNED_INDEX_H
#define LEVELDB_LEARNED_INDEX_H


#include <vector>
#include <cstring>
#include "koo/util.h"
#include "koo/plr.h"
#include "port/port.h"
#include "koo/koo.h"


using std::string;
using leveldb::Slice;
using leveldb::Version;
using leveldb::FileMetaData;


namespace leveldb { class FileMetaData; }

namespace koo {


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
				bool deleted_not_atomic;
				std::atomic<bool> deleted;
				port::Mutex mutex_delete_;
				bool merged;
				std::atomic<bool> retraining;
    public:
				uint64_t file_number;
				bool error_extented = false;
        // is the data of this model filled (ready for learning)
        bool filled;

        // Learned linear segments and some other data needed
        std::vector<Segment> string_segments;
        uint64_t min_key;
        uint64_t max_key;
        uint64_t size;			// SST내 entry 개수

    public:
        bool is_retrained_ = false;
        std::vector<Segment> string_segments_bak;
        Segment GetSegment(int offset) {
                return string_segments[offset];
        }
        size_t GetSegmentSize() {
                return string_segments.size();
        }
        // all keys in the file/level to be leraned from
        std::vector<uint64_t> *string_keys = nullptr;
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

        explicit LearnedIndexData(uint64_t number) : file_number(number), error(koo::learn_model_error), learned(false), 
						deleted(false), deleted_not_atomic(false),
						aborted(false), learning(false), learned_not_atomic(false), filled(false), level(0), cost(0), 
						retraining(false),
						merged(false) {};

        LearnedIndexData(const LearnedIndexData& other) = delete;
        ~LearnedIndexData();
        bool Deleted();
        void MarkDelete();

        // Inference function. Return the predicted interval.
        // If the key is in the training set, the output interval guarantees to include the key
        // otherwise, the output is undefined!
        // If the output lower bound is larger than MaxPosition(), the target key is not in the file
        std::pair<uint64_t, uint64_t> GetPosition(Slice& key);
        uint64_t MaxPosition() const;
        double GetError() const;
        void SetError(double cur_error, uint64_t extra_error);

				void SetMergedModel(std::vector<Segment>& segs);
				bool Merged();
				void FreezeModel();
				void UnfreezeModel();
				bool SetRetraining();
        
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
        LearnedIndexData* GetModelImm(int number);
        LearnedIndexData* GetModelForLookup(int number);
				void DeleteModel(int number);
        ~FileLearnedIndexData();
    };

    class LevelLearnedIndexData {
     private:
      leveldb::port::Mutex mutex;
      std::vector<LearnedIndexData*> level_learned_index_data;
     public:

    };


}

#endif //LEVELDB_LEARNED_INDEX_H
