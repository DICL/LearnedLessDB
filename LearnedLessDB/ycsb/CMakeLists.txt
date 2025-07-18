cmake_minimum_required(VERSION 3.10)

project(hyperleveldb-ycsb)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_FLAGS "-O3 -DNDEBUG")
#set(CMAKE_C_FLAGS "-Wall -Wsign-compare -g3")

##
# Compiler options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -fno-rtti ")

link_directories(../.)

# Add the directories for the ycsb target
add_executable(ycsb
  src/main.cc
  src/rocksdb_client.cc
  core/core_workload.cc
)

# Include directories for the ycsb target
target_include_directories(ycsb PRIVATE
  # Add your desired include directories here
  ../include
  ../.
  ${PROJECT_SOURCE_DIR}
)

# Link libraries for the ycsb target
target_link_libraries(ycsb
  pthread
  stdc++fs
  hyperleveldb
  dl
  #zstd
  #lz4
  #bz2
  snappy
  z
)

