#!/bin/bash
cd /model/mosaicml/llm-foundry/scripts
source venv/bin/activate

# Specify the IP address of the master node and an array of slave nodes
master_node="10.2.76.52"
slave_nodes=("10.2.76.51")  # Add more IPs as needed

function run_slave_node() {
    local world_size=16
    local master_addr=$master_node
    #local ssh_port=2200
    local composer_port=2221
    local params="13b"

    local rank=1
    for slave_node in "${slave_nodes[@]}"; do
        ssh $slave_node \
            bash /model/mosaicml/llm-foundry/scripts/run.sh $world_size $rank $master_addr $composer_port $params &
	rank=$((rank+1))
    done
}

function run_master_node() {
    local world_size=16
    local rank=0
    local master_addr=0.0.0.0
    local composer_port=2221
    local params="13b"

    bash /model/mosaicml/llm-foundry/scripts/run.sh $world_size $rank $master_addr $composer_port $params
}

function main() {
    run_slave_node
    run_master_node

    wait
}

main
