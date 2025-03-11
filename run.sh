#!/bin/bash

experiments=(
	'Philly' 
	'ali20'
	'Venus'
)

selector=fifo

declare -a configs=(
	# "gandiva clustering"
	"defragS clustering"

	# "${selector} consolidate"
	# "${selector} FGD"
	# "${selector} stBestFit"
	# "${selector} random"
	# "${selector} dotProd"
	# "${selector} clustering"
	# "${selector} worstFit"
)

for experiment in "${experiments[@]}"; do
	if [ "$experiment" == "Philly" ] || [ "$experiment" == "ali20" ]; then
		experiment_name="$experiment"
	else
		experiment_name="${experiment}_Sept"
	fi

	log="./log/impact_of_palcement_for_DeFragS/${experiment_name}"
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