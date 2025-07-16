cmake_minimum_required(VERSION 3.10)

project(hyperbourbon-ycsb)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_FLAGS "-O3 -DNDEBUG")
#set(CMAKE_C_FLAGS "-Wsign-compare -g3 -O3 -DNDEBUG")

##
# Compiler options
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wfatal-errors ")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I/koo/Bourbon/include ")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17 -march=native -fno-rtti ")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DROCKSDB_FALLOCATE_PRESENT ")

#link_directories(/koo/Bourbon/build)
link_directories(/koo/HyperBourbon)

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
  /koo/HyperBourbon
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

