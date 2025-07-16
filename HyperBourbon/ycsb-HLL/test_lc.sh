#!/bin/bash

cd build

echo "./ycsb -w l,c"
./ycsb -w l,c > ../results/lc_t1.txt

sleep 10

echo "./ycsb -w l,c -t 4"
./ycsb -w l,c -t 4 > ../results/lc_t4.txt

sleep 10

echo "./ycsb -w l,c -t 8"
./ycsb -w l,c -t 8 > ../results/lc_t8.txt

sleep 10

echo "./ycsb -w l,c -t 16"
./ycsb -w l,c -t 16 > ../results/lc_t16.txt

sleep 10

echo "./ycsb -w l,c -t 24"
./ycsb -w l,c -t 24 > ../results/lc_t24.txt
