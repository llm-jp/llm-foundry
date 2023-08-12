#!/bin/bash

set -e

spmodel=$1
dstdir=$2
mkdir -p $dstdir
for jsonl_path in ${@:3:($#-2)}
do
  python tokenize_jsonl.py $spmodel $jsonl_path 2>> tokenize_jsonl.log | pigz -c > $dstdir/`basename -s .jsonl $jsonl_path`.bin.gz &
done
for ((i=0; i<(($# - 2 - `nproc` / 5)); i++))
do
  wait -n
done
