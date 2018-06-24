#!/bin/bash

pkill -9 python
export i=1
export MAX=10
while [ $i -le $MAX ];
do
    python agent.py $i &
    i=$((i + 1))
    sleep 3
done

