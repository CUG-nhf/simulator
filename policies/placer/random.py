import random

class RandomPlacement:
	def __init__(self, vc):
		self.name = 'random'
		self.vc = vc

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
		
		'''assign partially idle nodes -- Random'''
		avail_nodes = [node for node in nodes if node.free_gpus >= partial_node_nmu]
		if len(avail_nodes) > 0:
			node = random.choice(avail_nodes)
			alloc_nodes.append((node, partial_node_nmu))
			return True, alloc_nodes
		return False, alloc_nodes
	
	def place(self, job):
		vc_free_gpu_num = self.vc.vc_free_gpus()
		job_gpu_num = job['gpu_num']

		# Total Free GPU Check
		if vc_free_gpu_num < job_gpu_num:
			return False

		# TODO: Support for 4 GPU Nodes
		if self.vc._num_gpus_per_node != 8:
			raise NotImplementedError

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
