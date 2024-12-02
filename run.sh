#!/bin/bash

# python simulator.py -e='Uranus_Sept' -t='./data/Uranus'

# python simulator.py -e='Saturn_Sept' -t='./data/Saturn'

# python simulator.py -e='Earth_Sept' -t='./data/Earth'

# python simulator.py -e='Venus_Sept' -t='./data/Venus'

# if [ -f ../nohup.out ]; then
# 		rm ../nohup.out
# 	fi

# ohup python simulator.py -e='Philly' -t='./data/Philly/frag_ratio_1_2' -l='/data/nihaifeng/log/frag_ratio_1_2' -p='gandiva' -s='gandiva' > ../nohup/nohup_nochangeration.out &
nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/prob_0_1/deFrag' -p='gandiva' -s='gandiva' > ../nohup/nohup_ratio_0P1.out &

# placer_ls=('FGD' 'consolidate')

# for placer in "${placer_ls[@]}"; do
# 	# 使用当前的 placer 运行 nohup 命令
#     nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/test/noDeFrag' -p="$placer" --sweep > "../nohup/${placer}_noDeFrag.out" &
# 	nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/test/deFrag' -p="$placer" --sweep -d > "../nohup/${placer}_DeFrag.out" &
# done
