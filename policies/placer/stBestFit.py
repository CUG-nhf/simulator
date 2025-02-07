import sys

class SpatioTemporalBestFit:
	def __init__(self, vc):
		self.vc = vc
		self.name = 'stBestFit'

	def calculateFitnessScore(self, node, job, node_free_gpu, job_req_gpu):
		return	(node_free_gpu-job_req_gpu)/node.num_gpus + (abs(node.getLargestReaminTime()-job['remain']))/(max(job['remain'], node.getLargestReaminTime()))  #+ 0.1 
		
	'''Spatio-Temporal Best-Fit placement'''
	def nodeSelect(self, job, job_gpu_num, node_list):
		alloc_nodes = []
		complete_node_num = job_gpu_num // 8
		partial_node_nmu = job_gpu_num % 8

		nodes = sorted(node_list, key=lambda x: x.free_gpus, reverse=True)

		'''assign completely idle nodes -- Consolidate'''
		while complete_node_num > 0:
			if len(nodes) > 0 and nodes[0].free_gpus == 8:
				alloc_nodes.append((nodes[0], 8))
				complete_node_num -= 1
				nodes.pop(0)
			else:
				return False, alloc_nodes
			
		if partial_node_nmu == 0:
			return True, alloc_nodes

		'''assign partially idle nodes -- Spatio-Temporal Best-Fit'''
		# 1) Filter out unavailable nodes
		nodes = [node for node in nodes if node.free_gpus >= partial_node_nmu]
		if len(nodes) == 0:
			return False, alloc_nodes
		# Assign Job to node
		target_node = None
		node_score = sys.float_info.max
		for node in nodes:
			node_free_gpus = node.free_gpus
			# 对可用节点进行打分排序，选择分数最小的节点：剩余时间接近，空闲卡数量少
			tmp_node_score = self.calculateFitnessScore(node, job, node_free_gpus, partial_node_nmu)
			if tmp_node_score < node_score:
				target_node = node
				node_score = tmp_node_score
		alloc_nodes.append((target_node, partial_node_nmu))

		return True, alloc_nodes

	def place(self, job):
		vc_free_gpu_num = self.vc.vc_free_gpus()
		job_gpu_num = job['gpu_num']

		# Total Free GPU Check
		if vc_free_gpu_num < job_gpu_num:
			return False

		select_flag, alloc_nodes = self.nodeSelect(job, job_gpu_num, self.vc.avail_node_list())

		''' Placement '''
		if select_flag:
			for (node, req_gpu) in alloc_nodes:
				node.allocate_gpu(req_gpu)
				node.add_job(job, req_gpu)
				job['nodes'].append({node.node_name: req_gpu})
			return True
		else:
			return False
