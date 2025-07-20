//
// Created by daiyi on 2020/03/23.
//

#include <fcntl.h>
#include <sys/stat.h>
#include <cstring>
#include "koo/Vlog.h"
#include "koo/util.h"
//#include "util/coding.h"
#include "koo/koo.h"

using std::string;



const int buffer_size_max = 240 * 1024;
const int V_BUFFER_SIZE = 256 * 1024;

namespace koo {

VLog::VLog(const std::string& vlog_name) : writer(nullptr), reader(nullptr), current_pos(0), count_pos(0) {
  koo::env->NewWritableFile(vlog_name, &writer);
  koo::env->NewRandomAccessFile(vlog_name, &reader);
  struct ::stat file_stat;
  ::stat(vlog_name.c_str(), &file_stat);
  buffer = (char*)calloc(V_BUFFER_SIZE, sizeof(char));
  vlog_size = file_stat.st_size;
  vlog_flushed = file_stat.st_size;
}

uint64_t VLog::AddRecord(const Slice& key, const Slice& value) {
  std::string buf;
  PutLengthPrefixedSlice(&buf, key);
  PutVarint32(&buf, value.size());
  size_t tmp_size = buf.size();
  buf.append(value.data(), value.size());

  uint64_t pos = current_pos.fetch_add(buf.size());
  while(pos > buffer_size_max) {
  	while (current_pos > buffer_size_max) { }
    pos = current_pos.fetch_add(buf.size());
  }
 
  uint64_t result = vlog_size + pos + tmp_size;

  std::memcpy(buffer + pos, buf.data(), buf.size());


  if (pos + buf.size() > buffer_size_max) {
    while (count_pos != pos) {}
    Flush(pos + buf.size());
    std::memset(buffer, 0, V_BUFFER_SIZE);
    count_pos = 0;
    current_pos = 0;
  } else {
    count_pos.fetch_add(buf.size());
  }
  return result;
}

string VLog::ReadRecord(uint64_t address, uint32_t size) {
  if (address >= vlog_size.load(std::memory_order_relaxed)) {
    std::unique_lock<SpinLock> lock(s_mu_);
    if (address >= vlog_size)
      return string(buffer + address - vlog_size, size);
  }

  char* scratch = new char[size];
  Slice value;
  reader->Read(address, size, &value, scratch);
  string result(value.data(), value.size());
  delete[] scratch;
  return result;
}

void VLog::Flush(uint64_t s) {
  std::unique_lock<SpinLock> lock(s_mu_);
  vlog_size += s;
  Slice buf(buffer, s);
  writer->Append(buf);
  writer->Flush();
}

void VLog::Sync() {
  //need to write buffer
  if (count_pos > 0) {
  	std::unique_lock<koo::SpinLock> lock(s_mu_);
  	vlog_size += count_pos;
  	Slice buf(buffer, count_pos);
  	writer->Append(buf);
  	writer->Flush();
	}

  writer->Sync();
}

VLog::~VLog() {
  Sync();
}

}
