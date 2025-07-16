#!/bin/bash

cd build

echo "./ycsb -w l,c -t 1"
./ycsb -w l,c -t 1 > ../results/11.13/lc_100G_20M_1t.txt

sleep 10

echo "./ycsb -w l,c -t 2"
./ycsb -w l,c -t 2 > ../results/11.13/lc_100G_20M_2t.txt

sleep 10

echo "./ycsb -w l,c -t 4"
./ycsb -w l,c -t 4 > ../results/11.13/lc_100G_20M_4t.txt

sleep 10

echo "./ycsb -w l,c -t 8"
./ycsb -w l,c -t 8 > ../results/11.13/lc_100G_20M_8t.txt

sleep 10

echo "./ycsb -w l,c -t 16"
./ycsb -w l,c -t 16 > ../results/11.13/lc_100G_20M_16t.txt

sleep 10

echo "./ycsb -w l,a -t 1"
./ycsb -w l,a -t 1 > ../results/11.13/la_100G_20M_1t.txt

sleep 10

echo "./ycsb -w l,a -t 2"
./ycsb -w l,a -t 2 > ../results/11.13/la_100G_20M_2t.txt

sleep 10

echo "./ycsb -w l,a -t 4"
./ycsb -w l,a -t 4 > ../results/11.13/la_100G_20M_4t.txt

sleep 10

echo "./ycsb -w l,a -t 8"
./ycsb -w l,a -t 8 > ../results/11.13/la_100G_20M_8t.txt

sleep 10

echo "./ycsb -w l,a -t 16"
./ycsb -w l,a -t 16 > ../results/11.13/la_100G_20M_16t.txt
