#!/bin/bash

chmod +x ./*.sh
chmod +x ./TrimTester

./trim_periodic.sh &
./drop_cache.sh &

mkdir -p ./test/

rm -rf ./test/*

while true
do
	if ./TrimTester ./test/; then
		rm -r ./test/*
	else
		echo "Corruption found"
		break
	fi
done
