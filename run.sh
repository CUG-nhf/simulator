#!/bin/bash

# python simulator.py -e='Uranus_Sept' -t='./data/Uranus'

# python simulator.py -e='Saturn_Sept' -t='./data/Saturn'

# python simulator.py -e='Earth_Sept' -t='./data/Earth'

# python simulator.py -e='Venus_Sept' -t='./data/Venus'

# placer_ls=('FGD' 'consolidate' 'random')
# scheduler_ls = ('fifo' 'sjf' '')
# for placer in "${placer_ls[@]}"; do
# 	# 使用当前的 placer 运行 nohup 命令
#     nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/test/noDeFrag' -p="$placer" --sweep > "../nohup/${placer}_noDeFrag.out" &
# 	nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/test/deFrag' -p="$placer" --sweep -d > "../nohup/${placer}_DeFrag.out" &
# done

###################  ###################
log='/data/nihaifeng/log/test'
output_dir="${log}/nohup"
mkdir -p ${output_dir}

declare -a configs=(
    "defragS sdf"
	# "defragS fifo"
	# "gandiva fifo"
    # "fifo FGD"
	# "fifo consolidate"
)
for config in "${configs[@]}"; do
    read -r scheduler placer <<< "$config"
    nohup python simulator.py -e='Philly' -t='./data/Philly' -l=${log} -s="${scheduler}" -p="${placer}" > ${output_dir}/${scheduler}_${placer}.out &
done
