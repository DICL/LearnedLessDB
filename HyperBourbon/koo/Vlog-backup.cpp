//
// Created by daiyi on 2020/03/23.
//

#include <fcntl.h>
#include <sys/stat.h>
#include "koo/Vlog.h"
#include "koo/util.h"
//#include "util/coding.h"
#include "koo/koo.h"

using std::string;



const int buffer_size_max = 300 * 1024;

namespace koo {

VLog::VLog(const std::string& vlog_name) : writer(nullptr), reader(nullptr) {
    koo::env->NewWritableFile(vlog_name, &writer);
    koo::env->NewRandomAccessFile(vlog_name, &reader);
    buffer.reserve(buffer_size_max * 2);
    struct ::stat file_stat;
    ::stat(vlog_name.c_str(), &file_stat);
    vlog_size = file_stat.st_size;
#if THREADSAFE
		vlog_flushed = file_stat.st_size;
#endif
}

#if THREADSAFE
/*void PrintString(std::string s){
	for(size_t i = 0; i < s.size(); i++)
		if((s.data()[i] <= 'z' && s.data()[i] >= 'a') || s.data()[i] <= 'Z' && s.data()[i] >= 'A')
		  fprintf(stderr, "%c", s.data()[i]);
		else
			fprintf(stderr, " |%d| ", s.data()[i]);
		fprintf(stderr, "\n");
}*/

uint64_t VLog::AddRecord(const Slice& key, const Slice& value) {
#if AC_TEST && MC_DEBUG
		std::chrono::system_clock::time_point StartTime;
		if (koo::count_compaction_triggered_after_load) {
			StartTime = std::chrono::system_clock::now();
		}
#endif
	  std::string buf;
    PutLengthPrefixedSlice(&buf, key);
    PutVarint32(&buf, value.size());
    size_t tmp_size = buf.size();
    buf.append(value.data(), value.size());
    
    //MutexLock l(&mu_);
    s_mu_.lock();
    uint64_t result = vlog_size + tmp_size;
    writer->Append(buf);
    vlog_size += buf.size();
    //why do we need fflush? it does not guarantee anything.
    /*if (vlog_size - vlog_flushed > 300*1024) {
    	writer->Flush();
		}*/
    
    //uint64_t result = vlog_size + buffer.size() + tmp_size;
    //buffer.append(buf.data(), value.size() + tmp_size);

    //if (buffer.size() >= buffer_size_max) Flush();
    s_mu_.unlock();
#if AC_TEST && MC_DEBUG
		if (koo::count_compaction_triggered_after_load) {
			std::chrono::nanoseconds nano = std::chrono::system_clock::now() - StartTime;
			koo::time_tAppend += nano.count();
			koo::num_tAppend++;
		}
#endif
    return result;
    /*PutLengthPrefixedSlice(&buffer, key);
    PutVarint32(&buffer, value.size());
    uint64_t result = vlog_size + buffer.size();
    buffer.append(value.data(), value.size());

    if (buffer.size() >= buffer_size_max) Flush();
    return result;*/
}
#else
uint64_t VLog::AddRecord(const Slice& key, const Slice& value) {
    PutLengthPrefixedSlice(&buffer, key);
    PutVarint32(&buffer, value.size());
    uint64_t result = vlog_size + buffer.size();
    buffer.append(value.data(), value.size());

    if (buffer.size() >= buffer_size_max) Flush();
    return result;
}
#endif

string VLog::ReadRecord(uint64_t address, uint32_t size) {
    if (address >= vlog_size) {
    	fprintf(stderr, "Something Wring!! vlog_size: %lu, address: %lu\n", vlog_size, address);
    	return string(buffer.c_str() + address - vlog_size, size);
		}

    char* scratch = new char[size];
    Slice value;
    reader->Read(address, size, &value, scratch);
    string result(value.data(), value.size());
    delete[] scratch;
    return result;
}

void VLog::Flush() {
    if (buffer.empty()) return;

    vlog_size += buffer.size();
    writer->Append(buffer);
    writer->Flush();
    buffer.clear();
    buffer.reserve(buffer_size_max * 2);
}

void VLog::Sync() {
    Flush();
    writer->Sync();
}

VLog::~VLog() {
    Flush();
}































}
