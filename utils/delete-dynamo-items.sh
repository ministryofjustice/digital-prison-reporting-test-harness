#!/bin/bash

# Start and end values
checkpoint_start=10
checkpoint_end=60

# Pad the start and end values with leading zeros
start_padded=$(printf "%012d" "$checkpoint_start")
end_padded=$(printf "%012d" "$checkpoint_end")

# Iterate over the range of values
for ((value=checkpoint_start; value<=checkpoint_end; value++))
do
    
    checkpoint_id=$(printf "%012d" "$value")
    echo "Deleting command with value: $checkpoint_id"
    aws  dynamodb delete-item --table-name dpr-reporting-hub-test  --key "{\"leaseKey\":{\"S\":\"shardId-$checkpoint_id\"}}" --region eu-west-2
done
