#!/bin/bash

make -j && sudo make install
cd ycsb/build
rm -rf *
cmake ..
make -j10
cd ../scripts
