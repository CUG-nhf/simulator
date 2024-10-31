#!/bin/bash

# python simulator.py -e='Uranus_Sept' -t='./data/Uranus'

# python simulator.py -e='Saturn_Sept' -t='./data/Saturn'

# python simulator.py -e='Earth_Sept' -t='./data/Earth'

# python simulator.py -e='Venus_Sept' -t='./data/Venus'

if [ -f ../nohup.out ]; then
		rm ../nohup.out
	fi

# nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/frag_ratio_1_2' -p='consolidate' --sweep > ../nohup.out &
# nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/frag_ratio_1_2' -p='random' --sweep > ../nohup.out &
# nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/frag_ratio_1_2' -p='FGD' --sweep > ../nohup.out &
nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/tmp/noDeFra' -p='consolidate' --sweep > ../nohup.out &


# nohup python simulator.py -e='Test' -t='./data/Test' -l='/data/nihaifeng/log/test' -p='consolidate' --sweep > ../nohup.out &


# placer_ls=('consolidate' 'random' 'FGD')

# for placer in "${placer_ls[@]}"; do
#     # 检查文件是否存在，如果存在则删除
#     if [ -f "../${placer}_nohup.out" ]; then
#         rm "../${placer}_nohup.out"
#     fi
    
#     # 使用当前的 placer 运行 nohup 命令
#     nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/defragmentation/algorithm_1' -p="$placer" --sweep > "../${placer}_nohup.out" &
# done
