#!/bin/sh

set -e

srcdir=/data/data_v001_20230706
dstdir=./tokenized/$1
mkdir -p $dstdir
for dataset in code_stack ja_wiki en_wiki ja_cc en_pile
do
  echo `date "+%Y/%m/%d %H:%M:%S"` "$srcdir/$dataset.tar.gz -> $dstdir started" && \
  pigz -d -c $srcdir/$dataset.tar.gz | tar xf - -O | parallel --pipe -j 16 --round -u --blocksize 15000000 python tokenize_jsonl.py $1 | pigz -c > $dstdir/$dataset.bin.gz && \
  echo `date "+%Y/%m/%d %H:%M:%S"` "$srcdir/$dataset.tar.gz -> $dstdir done" &
done
