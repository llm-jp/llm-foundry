#!/bin/sh

set -e

srcdir=/data/data_v001_20230706
dstdir=./tokenized/$1
for dataset in code_stack ja_wiki en_wiki ja_cc en_pile
do
  echo `date "+%Y/%m/%d %H:%M:%S"` tokenizing $srcdir/$dataset.tar.gz
  mkdir -p $dstdir/$dataset
  tar zxf $srcdir/$dataset.tar.gz -O | parallel --pipe -j 32 --round --blocksize 15000000 python tokenize_jsonl.py $1 $dstdir/$dataset/'{%}.bin.gz'
  echo `date "+%Y/%m/%d %H:%M:%S"` "done"
done
