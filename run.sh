#!/bin/bash

months=(
	'Sept'
	# 'July'
	# 'June'
	# 'all'
)

experiments=(
	'Philly' 
	# 'ali20'
	# 'Venus'
	# 'Saturn' 
	# 'Earth' 
	# 'Uranus'
)

declare -a configs=(
	# "defragS dynamic_noDefrag"
	"defragS dynamic"
	"gandiva dynamic"
	"dynamic FGD"
	"dynamic consolidate"

	# "defragS fifo"
	# "gandiva fifo"
	# "fifo FGD"
	# "fifo consolidate"

	# "defragS sdf"
	# "defragS sjf"
	# "gandiva sjf"
	# "sjf FGD"
	# "sjf consolidate"
)

for month in "${months[@]}"; do
	for experiment in "${experiments[@]}"; do
		if [ "$experiment" == "Philly" ] || [ "$experiment" == "ali20" ]; then
			experiment_name="$experiment"
		else
			experiment_name="${experiment}_${month}"
		fi

		log="./log/tmp/${experiment_name}_mutation_25"
		mkdir -p "$log"

		output_dir="${log}/nohup"
		mkdir -p "$output_dir"

		for config in "${configs[@]}"; do
			scheduler=$(echo "$config" | awk '{print $1}')
			placer=$(echo "$config" | awk '{print $2}')

			rm -rf "${log}/${experiment_name}_${scheduler}_${placer}"

			nohup python simulator.py \
				-e="$experiment_name" \
				-t="./data/${experiment}" \
				-l="${log}" \
				-s="${scheduler}" \
				-p="${placer}" \
				--mutation \
				> "${output_dir}/${experiment_name}_${scheduler}_${placer}.out" &
		done
	done
done
#  --mutation: This parameter Only has effect on Philly