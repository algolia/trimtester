#!/bin/bash

./trim_periodic.sh &
./drop_cache.sh &

mkdir ./test/ || true

rm -r ./test/*

while true
do
	if ./TrimTester ./test/; then
		rm -r ./test/*
	else
		echo "Corruption found"
		break
	fi
done
