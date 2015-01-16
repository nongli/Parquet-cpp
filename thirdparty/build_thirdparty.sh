#!/bin/bash

set -x
set -e
TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/versions.sh
PREFIX=$TP_DIR/installed

################################################################################

if [ "$#" = "0" ]; then
  F_ALL=1
else
  # Allow passing specific libs to build on the command line
  for arg in "$*"; do
    case $arg in
      "gtest")      F_GTEST=1 ;;
      "lz4")        F_LZ4=1 ;;
      "snappy")     F_SNAPPY=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

# Determine how many parallel jobs to use for make based on the number of cores
if [[ "$OSTYPE" =~ ^linux ]]; then
  PARALLEL=$(grep -c processor /proc/cpuinfo)
elif [[ "$OSTYPE" == "darwin"* ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib"

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
ln -sf lib "$PREFIX/lib64"

# use the compiled tools
export PATH=$PREFIX/bin:$PATH

# build gtest
if [ -n "$F_ALL" -o -n "$F_GTEST" ]; then
  cd $GTEST_DIR
  echo "Building gtest"
  CXXFLAGS=-fPIC cmake .
  make -j$PARALLEL
fi

# build lz4
if [ -n "$F_ALL" -o -n "$F_LZ4" ]; then
  cd $LZ4_DIR
  echo "Building lz4"
  CFLAGS=-fPIC cmake -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX $LZ4_DIR
  make -j$PARALLEL install
fi

# build snappy
if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
  cd $SNAPPY_DIR
  echo "Building snappy"
  ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"

