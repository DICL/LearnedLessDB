#!/bin/bash

autoreconf -i
./configure
make -j32 && sudo make install
mkdir ycsb/build
cd ycsb/build
rm -rf *
cmake ..
make -j10
cd ../scripts
