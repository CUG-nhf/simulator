#!/bin/bash

months=(
	'Sept'
	# 'July'
	# 'June'
	# 'all'
)

experiments=(
	'Philly' 
	'ali20'
	'Venus'
	# 'Earth' 
	# 'Saturn' 
	# 'Uranus'
)

selector=fifo

declare -a configs=(
	# "gandiva new"
	#"defragS ${selector}_FGD"
	"defragS stBestFit"

	# "${selector} consolidate"
	# "${selector} FGD"
	"${selector} stBestFit"
)

for month in "${months[@]}"; do
	for experiment in "${experiments[@]}"; do
		if [ "$experiment" == "Philly" ] || [ "$experiment" == "ali20" ]; then
			experiment_name="$experiment"
		else
			experiment_name="${experiment}_${month}"
		fi

		log="./log/noDuration_45/${experiment_name}" # ${selector}
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
				> "${output_dir}/${experiment_name}_${scheduler}_${placer}.out" &
		done
	done
done
#  --mutation: This parameter Only has effect on Philly