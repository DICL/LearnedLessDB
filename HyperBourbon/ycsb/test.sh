#!/bin/bash

for var in {0..1..1}
do
	cd /nvmeof-mnt
	rm -rf *

	echo "sleep"
	sleep 10

	cd -
	./ycsb -w load,c -n 10000000
	sleep 3
done
