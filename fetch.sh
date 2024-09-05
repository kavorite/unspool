#!/bin/bash

f=$(date --iso -d 'yesterday' | tr -d '-')
t=$(date --iso | tr -d '-')
while getopts ":f:t:" opt; do 
    case $opt in
        f)
            f=$OPTARG
        ;;
        t)
            t=$OPTARG
        ;;
    esac
done
shift $((OPTIND-1))

read -r -d '' qry << EOQ
    to_entries
    | map(.value)
    | flatten(1)
    | map(select(.date >= "$f"))
    | map(select(.date <= "$t"))
    | map(select(.feed == "TOPS"))
    | sort_by(.date)
EOQ

hist='https://iextrading.com/api/1.0/hist'
entries=$(curl -s "$hist" | jq -c "$qry")
echo "$entries" | jq -r '.[] | "\(.date) \(.link)"' | while read -r date link; do
    output_file="hist/$date.parquet"
    echo "Creating $output_file"
    curl -Ls "$link" | ./unspool "$@" - > "${output_file}"
done
