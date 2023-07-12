#!/bin/bash

date=$(date +"%Y%m%d%s")
exec > >(tee scripts/log/"$date"_experiment.log) 2>&1

echo "start at $(date '+%B %d %Y %T')"

set -ex

if [ $# -ge 1 ]; then
    backend=$1
else
    backend=cephfs
fi

user=luoxh
client=hp081.utah.cloudlab.us
leader=hp094.utah.cloudlab.us
follower=(hp086.utah.cloudlab.us hp100.utah.cloudlab.us)
replica=($leader ${follower[@]})

dir=/data/go-ycsb  # YCSB binary directory
db_dir=/data/rqlite-v7.21.4-linux-amd64  # rqlite binary directory
db_base=/data/sqlite_base/ycsb.db  # sqlite base snapshot directory
output_dir=/data/result/sqlite  # experiemnt results directory
local_output_dir=~/result/sqlite  # local experiemnt results directory

declare -A pub2pri
declare -A sqlite_svr_pid

for r in ${replica[@]}
do
    sqlite_svr_pid[$r]=0
    echo ${sqlite_svr_pid[$r]}
done

pub2pri[$client]=10.10.1.3
pub2pri[$leader]=10.10.1.4
pub2pri[${follower[0]}]=10.10.1.2
pub2pri[${follower[1]}]=10.10.1.1

function prepare_run() {
    for r in ${replica[@]}
    do
        # ssh -o StrictHostKeyChecking=no $user@$r "echo 600M | sudo tee /sys/fs/cgroup/memory/ycsb/memory.limit_in_bytes"
        ssh -o StrictHostKeyChecking=no $user@$r "echo 3 | sudo tee /proc/sys/vm/drop_caches"
        ssh -o StrictHostKeyChecking=no $user@$r "sudo rm -rf $db_dir/node.*"
    done
}

function prepare_load() {
    memlim=$1
    for r in ${replica[@]}
    do
        echo "mem limit ${memlim}B"
        # ssh -o StrictHostKeyChecking=no $user@$ip "echo 200M | sudo tee /sys/fs/cgroup/memory/ycsb/memory.limit_in_bytes"
        ssh -o StrictHostKeyChecking=no $user@$ip "echo 3 | sudo tee /proc/sys/vm/drop_caches"
        ssh -o StrictHostKeyChecking=no $user@$ip "sudo rm -rf $db_dir/node.*"
    done
}

function run_sqlite_server() {
    mode=$1
    th=$2

    for r in ${replica[@]}
    do
        kill_sqlite_server $r || true
    done

    ssh -o StrictHostKeyChecking=no $user@$leader "sudo $extra_lib \
        $db_dir/rqlited \
        -node-id ${pub2pri[$leader]} \
        -http-addr ${pub2pri[$leader]}:4001 \
        -raft-addr ${pub2pri[$leader]}:4002 \
        $db_dir/node.${pub2pri[$leader]} >$db_dir/sqlite_svr.log 2>&1" &
    sqlite_svr_pid[$leader]=$!

    for f in ${follower[@]}
    do
        ssh -o StrictHostKeyChecking=no $user@$f "sudo $extra_lib \
            $db_dir/rqlited \
            -node-id ${pub2pri[$f]} \
            -http-addr ${pub2pri[$f]}:4001 \
            -raft-addr ${pub2pri[$f]}:4002 \
            -join http://${pub2pri[$leader]}:4001 \
            $db_dir/node.${pub2pri[$f]} >$db_dir/sqlite_svr.log 2>&1" &
        sqlite_svr_pid[$f]=$!
    done
}

function kill_sqlite_server() {
    ip=$1
    ssh -o StrictHostKeyChecking=no $user@$ip "pid=\$(sudo lsof -i :4001 | awk 'NR==2 {print \$2}') ; \
        sudo kill -2 \$pid 2> /dev/null || true "
    wait ${sqlite_svr_pid[$ip]}
    ssh -o StrictHostKeyChecking=no $user@$ip "ps aux | grep $db_dir/rqlited"
}

function run_sqlite_cli() {
    mode=$1
    wl=$2
    th=$3
    rccnt=$4
    opcnt=$5
    path=$6
    yaml=$7

    ssh -o StrictHostKeyChecking=no $user@$client "mkdir -p $output_dir"
    
    ssh -o StrictHostKeyChecking=no $user@$client "sudo $dir/bin/go-ycsb $mode rqlite \
        -P $dir/workloads/$wl \
        -p recordcount=$rccnt \
        -p operationcount=$opcnt \
        -p outputstyle=yaml \
        -p measurement.output_file=$output_dir/$yaml.yml"

    mkdir -p $local_output_dir/$mode
    scp $user@$client:$output_dir/$yaml.yml $local_output_dir/$mode/
}

function run_once() {
    mode=$1
    workload=$2
    thread=$3
    recordcount=$4
    operationcount=$5
    path=$6
    yaml=$7

    if [ $mode = load ]
    then
        prepare_load $(printf "%.0f" "$(echo "scale=0; $recordcount*128*0.3" | bc)")
    else
        prepare_run
    fi

    run_sqlite_server $mode $thread

    sleep 15

    if [ $mode = run ]
    then
        ssh -o StrictHostKeyChecking=no $user@$client "curl -v -XPOST ${pub2pri[$leader]}:4001/db/load -H \"Content-type: application/octet-stream\" --data-binary @$db_base"
    fi

    run_sqlite_cli $mode $workload $thread $recordcount $operationcount $path $yaml

    for r in ${replica[@]}
    do
        kill_sqlite_server $r
    done
}

function run_ycsb() {
    iter=3
    workloads=(workloada workloadb workloadc workloadd workloadf)
    operation_M=(10 5 10 5 10)

    for (( i=1; i<=$iter; i++ ))
    do
        for idx in ${!workloads[@]}
        do
            wl=${workloads[$idx]}
            operationcnt=$((${operation_M[$idx]} * 10000))

            expname=sqlite_"$wl"_"$backend"_trail"$i"
            echo "Running experiment $expname"
            sleep 1
            run_once run $wl 1 1000000 $operationcnt $db_dir $expname
        done
    done
}

function run_load() {
    iter=3

    n_threads=(1)
    record_M=(1)

    # n_threads=(1)
    # record_M=(6)

    for (( i=1; i<=$iter; i++ ))
    do
        for idx in ${!n_threads[@]}
        do
            thread=${n_threads[$idx]}
            recordcnt=$((${record_M[$idx]} * 500000))

            expname=sqlite_load_"$backend"_th"$thread"_trail"$i"
            echo "Running experiment $expname"
            run_once load workloada $thread $recordcnt 0 $db_dir $expname
        done
    done
}

# run_load
run_ycsb

echo "end at $(date '+%B %d %Y %T')"
