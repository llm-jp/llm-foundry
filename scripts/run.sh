#!/bin/bash
cd /model/mosaicml/llm-foundry/scripts
source venv/bin/activate

readonly training_script="/model/mosaicml/llm-foundry/scripts/train/train.py"
readonly yaml_base_dir="/model/mosaicml/llm-foundry/scripts/"


function run() {
    local world_size=$1
    local rank=$2
    local addr=$3
    local port=$4
    local params=$5

    local training_script_args="$yaml_base_dir/mpt-13b-2node16gpu.yaml"

    composer \
        --verbose \
        --world_size $world_size \
        --node_rank $rank \
        --master_addr $addr \
        --master_port $port \
        $training_script \
        $training_script_args \
    data_local=my-copy-c4 train_loader.dataset.split=train_small eval_loader.dataset.split=val_small \
    save_folder=sh-test
}

run $@
