#!/bin/bash

set -e

./tokenize_jsonl.sh $1.model bin_data/$1/en_wiki /model/llm-jp-corpus/v1.0.1/sample/en_wiki/*.jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/ja_wiki /model/llm-jp-corpus/v1.0.1/sample/ja_wiki/*.jsonl

./tokenize_jsonl.sh $1.model bin_data/$1/code_stack /model/llm-jp-corpus/v1.0.1/sample/code_stack/*[012].jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/code_stack /model/llm-jp-corpus/v1.0.1/sample/code_stack/*[345].jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/code_stack /model/llm-jp-corpus/v1.0.1/sample/code_stack/*[6789].jsonl

./tokenize_jsonl.sh $1.model bin_data/$1/en_pile /model/llm-jp-corpus/v1.0.1/sample/en_pile/*[012].jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/en_pile /model/llm-jp-corpus/v1.0.1/sample/en_pile/*[345].jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/en_pile /model/llm-jp-corpus/v1.0.1/sample/en_pile/*[6789].jsonl

./tokenize_jsonl.sh $1.model bin_data/$1/ja_cc /model/llm-jp-corpus/v1.0.1/sample/ja_cc/*[012].jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/ja_cc /model/llm-jp-corpus/v1.0.1/sample/ja_cc/*[345].jsonl
./tokenize_jsonl.sh $1.model bin_data/$1/ja_cc /model/llm-jp-corpus/v1.0.1/sample/ja_cc/*[6789].jsonl
