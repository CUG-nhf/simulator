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


###################  这次运行的是把修改碎片整理时机 ###################
# placer='dt'
# scheduler='defragS'
# nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/PPT_51' -p="$placer" -s=${scheduler} > ../nohup/${scheduler}_${placer}.out & 

# def defragmentation(self): 
# 		if len(self.que_list) > 0 and self._vc.vc_free_gpus() > self.que_list[0]['gpu_num']:
# 			migrationMap = self._vc.defragmentation()
# 			self.last_defrag_time = self.time
# 			for job, source_node, target_node, job_req_gpu in migrationMap:
# 				print(f'''TIME:{self.time},VC:{self._vc.vc_name}-- {job['jobname']} FROM {source_node.node_name} MIGRATE TO {target_node.node_name} WITH {job_req_gpu} GPUs''')


###################  这次运行的修改随机种子 42 -> rsXX ###################
placer='rs50'
scheduler='defragS'
nohup python simulator.py -e='Philly' -t='./data/Philly' -l='/data/nihaifeng/log/PPT_51' -p="$placer" -s=${scheduler} > ../nohup/${scheduler}_${placer}.out & 