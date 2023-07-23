#!/bin/bash
cd /model/mosaicml/llm-foundry/scripts
source venv/bin/activate

# Specify the IP address of the master node and an array of slave nodes
master_node="10.2.76.52"
slave_nodes=("10.2.72.152")  # Add more IPs as needed

function kill_processes_on_port() {
    local port=2221  # Replace with the port number you want to free
    for node in $master_node "${slave_nodes[@]}"; do
        ssh $node "
            output=\$(lsof -i :$port)

            echo \"\$output\" | while IFS= read -r line
            do
                pid=\$(echo \$line | awk '{ print \$2 }')

                if [[ \$pid =~ ^[0-9]+$ ]]
                then
                    kill -9 \$pid
                    echo \"Killed process \$pid\"
                fi
            done
        "
    done
}


function run_python_script() {
    local script_path="/model/mosaicml/llm-foundry/scripts/clean_stream.py"  # Replace with the actual path to your Python script
    local env_script_path="/model/mosaicml/llm-foundry/scripts/venv/bin/activate"  # Replace with the actual path to your environment script
    for node in $master_node "${slave_nodes[@]}"; do
        ssh $node "source $env_script_path && python $script_path"
    done
}

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
    kill_processes_on_port
    run_python_script
    run_slave_node
    run_master_node

    wait
}

main

