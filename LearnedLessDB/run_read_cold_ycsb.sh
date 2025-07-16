#!/bin/bash

num_workloads=1
threads=16
fresh_write=0
evict=0
input_file_root="/koo/dataset/ycsb"
input_file_dir="${input_file_root}/labcfd_500M_10M_K16"

#input_file="${input_file_dir}/load_ycsb.txt"
#input_file="${input_file_dir}/a_ycsb.txt,${input_file_dir}/b_ycsb.txt,${input_file_dir}/c_ycsb.txt"
input_file="${input_file_dir}/c_ycsb.txt"
#input_file="${input_file_dir}/load_ycsb.txt,${input_file_dir}/c_ycsb.txt"
#input_file="${input_file_dir}/load_ycsb.txt,${input_file_dir}/a_ycsb.txt,${input_file_dir}/b_ycsb.txt,${input_file_dir}/c_ycsb.txt,${input_file_dir}/f_ycsb.txt,${input_file_dir}/d_ycsb.txt"

db="hyperlearningless"
date="02.13"
#output_file="l_500M-8Bvalue_t32"
output_file="c_500M-8Bvalue_10M_t16"
#output_file="abc_500M-8Bvalue_10M_t32"
#output_file="labcfd_500M-8Bvalue_10M_t32"

##################
make -j
sudo make install
##################

input_file2="${input_file_dir}/b_ycsb.txt"
output_file2="b_500M-8Bvalue_10M_t16"
input_file3="${input_file_dir}/a_ycsb.txt"
output_file3="a_500M-8Bvalue_10M_t16"
for num in 2; do
	./.libs/read_cold_ycsb --num_workloads=${num_workloads} \
								 --workloads=${input_file} \
								 --evict=${evict} \
								 --fresh_write=${fresh_write} \
								 --num_threads=${threads} > /koo/tests/${db}/${date}/${output_file}_AWR-${num}.txt
	./.libs/read_cold_ycsb --num_workloads=${num_workloads} \
								 --workloads=${input_file2} \
								 --evict=${evict} \
								 --fresh_write=${fresh_write} \
								 --num_threads=${threads} > /koo/tests/${db}/${date}/${output_file2}_AWR-${num}.txt
	./.libs/read_cold_ycsb --num_workloads=${num_workloads} \
								 --workloads=${input_file3} \
								 --evict=${evict} \
								 --fresh_write=${fresh_write} \
								 --num_threads=${threads} > /koo/tests/${db}/${date}/${output_file3}_AWR-${num}.txt
done

#for num in 4 5 6 7 8 9 10; do
#	./.libs/read_cold_ycsb --num_workloads=${num_workloads} \
#								 --workloads=${input_file} \
#								 --evict=${evict} \
#								 --num_threads=${threads} > /koo/tests/${db}/${date}/${output_file}-${num}.txt
#done

