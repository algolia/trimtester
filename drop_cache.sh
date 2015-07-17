#!/bin/bash

while true
do
	echo 3 > /proc/sys/vm/drop_caches
	sleep 120
done
