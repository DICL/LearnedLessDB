#!/bin/bash

cd build

echo "./ycsb -w l,a,c"
./ycsb -w l,a,c > ../results/lac_t1.txt

sleep 10

echo "./ycsb -w l,a,c -t 4"
./ycsb -w l,a,c -t 4 > ../results/lac_t4.txt

sleep 10

echo "./ycsb -w l,a,c -t 8"
./ycsb -w l,a,c -t 8 > ../results/lac_t8.txt

sleep 10

echo "./ycsb -w l,a,c -t 16"
./ycsb -w l,a,c -t 16 > ../results/lac_t16.txt

sleep 10

echo "./ycsb -w l,a,c -t 24"
./ycsb -w l,a,c -t 24 > ../results/lac_t24.txt
