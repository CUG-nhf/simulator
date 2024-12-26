#!/bin/bash

# python simulator.py -e='Uranus_Sept' -t='./data/Uranus'

# python simulator.py -e='Saturn_Sept' -t='./data/Saturn'

# python simulator.py -e='Earth_Sept' -t='./data/Earth'

# python simulator.py -e='Venus_Sept' -t='./data/Venus'


###################           ###################
log='/data/nihaifeng/log/test'
output_dir="${log}/nohup"
mkdir -p ${output_dir}
 
months='July'
experiments=(
	'Uranus' 
	'Saturn' 
	'Earth' 
	'Venus'
)

declare -a configs=(
	# "gandiva fifo"
    # "defragS fifo"
    # "fifo FGD"
	# "fifo consolidate"
	"gandiva sjf"
    "defragS sdf"
    "sjf FGD"
	"sjf consolidate"
)

for experiment in "${experiments[@]}"; do
    if [ "$experiment" == "Philly" ]; then
        experiment_name="$experiment"
    else
        experiment_name="${experiment}_${months}"
    fi

    for config in "${configs[@]}"; do
        scheduler=$(echo "$config" | awk '{print $1}')
        placer=$(echo "$config" | awk '{print $2}')

        nohup python simulator.py \
            -e="$experiment_name" \
            -t="/data/nihaifeng/code/HeliosArtifact/simulator/data/${experiment}" \
            -l="${log}" \
            -s="${scheduler}" \
            -p="${placer}" \
            --mutation > "${output_dir}/${experiment_name}_${scheduler}_${placer}.out" &
    done
done