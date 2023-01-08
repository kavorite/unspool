f=`date --iso | tr -d '-'`
t=`date --iso | tr -d '-'`
while getopts ":f:t:" opt; do 
    case $opt in
        f)
            f=$OPTARG
            shift 2
        ;;
        t)
            t=$OPTARG
            shift 2
        ;;
    esac
done

read -r -d '' qry << EOQ
    to_entries
    | map(.value)
    | flatten(1)
    | map(select(.date >= "$f"))
    | map(select(.date <= "$t"))
    | map(select(.feed == "TOPS"))
    | sort_by(.date)
    | map(.link)
    | join("\n")
EOQ
hist='https://iextrading.com/api/1.0/hist'
urls=`curl $hist | jq -r "$qry"`
while read -r link; do
    curl -Ls "$link" | ./unspool "$@" -
done <<< "$urls"

