# trimtester
This repository contains the source code of the testing script we have used to investigate this [trim issue](https://blog.algolia.com/when-solid-state-drives-are-not-that-solid/).

# Warning
This script writes a significant amount of data on the drives.

# Requirements
* g++
* libboost-thread
* libboost-system

# Testing
```bash
make
bash ./run.sh
```
