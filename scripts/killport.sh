#!/bin/bash

# lsofコマンドを実行し、出力を変数に保存
output=$(lsof -i :2221)

# 出力を行ごとに読み込む
echo "$output" | while IFS= read -r line
do
    # PIDを抽出（awkコマンドを使用）
    pid=$(echo $line | awk '{ print $2 }')

    # 抽出したPIDが数字であるかチェック
    if [[ $pid =~ ^[0-9]+$ ]]
    then
        # PIDが数字ならプロセスをkill
        kill -9 $pid
        echo "Killed process $pid"
    fi
done

