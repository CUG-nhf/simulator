#!/bin/bash

months=(
	'Sept'
	'July'
	'June'
	# 'all'
)

experiments=(
	'Philly'
	# 'ali20'
	# 'Uranus' 
	# 'Saturn' 
	# 'Earth' 
	# 'Venus'
)

declare -a configs=(
	"defragS dynamic"
	# "gandiva fifo"
    # "fifo FGD"
	# "fifo consolidate"
)

for month in "${months[@]}"; do
	for experiment in "${experiments[@]}"; do
		if [ "$experiment" == "Philly" ] || [ "$experiment" == "ali20" ]; then
			experiment_name="$experiment"
		else
			experiment_name="${experiment}_${month}"
		fi

		log="/data/nihaifeng/log/${experiment_name}"
		mkdir -p "$log"

		output_dir="${log}/nohup"
		mkdir -p "$output_dir"

		for config in "${configs[@]}"; do
			scheduler=$(echo "$config" | awk '{print $1}')
			placer=$(echo "$config" | awk '{print $2}')

			nohup python simulator.py \
				-e="$experiment_name" \
				-t="/data/nihaifeng/code/HeliosArtifact/simulator/data/${experiment}" \
				-l="${log}" \
				-s="${scheduler}" \
				-p="${placer}" \
				> "${output_dir}/${experiment_name}_${scheduler}_${placer}.out" &
		done
	done
done
#  --mutation: This parameter Only has effect on Philly