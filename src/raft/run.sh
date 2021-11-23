#!/usr/bin/env bash

function run_part1 {
  echo "Now Test Part 1"
  go test -run Election
}

function run_part2 {
  echo "Now Test Part 2"
  go test -run FailNoAgree
  go test -run ConcurrentStarts
  go test -run Rejoin
  go test -run Backup
}

function run_part3 {
  echo "Now Test Part 3"
  go test -run Persist1
  go test -run Persist2
  go test -run Persist3
}

function run_test {
    if [[ $1 == 1 ]]
    then
      run_part1
    elif [[ $1 == 2 ]]
    then
      run_part2
    elif [[ $1 == 3 ]]
    then
      run_part3
    else
      echo "Now Test Raft Impl, including part1, part2, part3"
      run_part1
      run_part2
      run_part3
    fi
}

run_test $1