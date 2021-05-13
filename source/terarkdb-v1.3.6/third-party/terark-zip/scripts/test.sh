#!/bin/bash

set -e # exit on error

if [ `uname` == Darwin ]; then
	cpuNum=`sysctl -n machdep.cpu.thread_count`
else
	cpuNum=`nproc`
fi

make -j$cpuNum test

# more test cases under google test framework
./gtest.sh
