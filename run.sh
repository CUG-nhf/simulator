#!/bin/bash

# python simulator.py -e='Uranus_Sept' -t='./data/Uranus'

# python simulator.py -e='Saturn_Sept' -t='./data/Saturn'

# python simulator.py -e='Earth_Sept' -t='./data/Earth'

# python simulator.py -e='Venus_Sept' -t='./data/Venus'

nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/tmp' -p='sdf' -s='defragS' > ../nohup/DFS_sdf.out & 
# 这次运行的是把修改碎片整理条件

# placer_ls=('FGD' 'consolidate' 'random')
# scheduler_ls = ('fifo' 'sjf' '')

# for placer in "${placer_ls[@]}"; do
# 	# 使用当前的 placer 运行 nohup 命令
#     nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/test/noDeFrag' -p="$placer" --sweep > "../nohup/${placer}_noDeFrag.out" &
# 	nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/test/deFrag' -p="$placer" --sweep -d > "../nohup/${placer}_DeFrag.out" &
# done
