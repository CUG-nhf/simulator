#!/bin/bash

# python simulator.py -e='Uranus_Sept' -t='./data/Uranus'

# python simulator.py -e='Saturn_Sept' -t='./data/Saturn'

# python simulator.py -e='Earth_Sept' -t='./data/Earth'

# python simulator.py -e='Venus_Sept' -t='./data/Venus'

if [ -f ../nohup.out ]; then
    rm ../nohup.out
fi

nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/prob_5' -p='consolidate' --sweep > ../nohup.out &

nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/prob_5' -p='random' --sweep > ../nohup.out &
