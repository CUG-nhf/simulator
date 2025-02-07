class FragmentationGradientDescent:
	def __init__(self, vc, jobPopulation):
		self.vc = vc
		self.name = 'FGD'
		# for FGD to calculate Job Population
		self.jobPopulation = jobPopulation
	
	def nodeGpuFragAmount(self, node_free_gpus):
		fragAmount = 0
		for job_gpu_num in self.jobPopulation:
			if node_free_gpus < job_gpu_num:
				fragAmount += node_free_gpus * self.jobPopulation[job_gpu_num]
		return fragAmount

	'''FGD placement'''

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

		'''assign partially idle nodes -- FGD'''

		# 1) Filter out unavailable nodes
		nodes = [node for node in nodes if node.free_gpus >= partial_node_nmu]
		if len(nodes) == 0:
			return False, alloc_nodes
		# Assign Job to node hypothetically
		node_score_list = []
		for node in nodes:
			nodeFragScore = self.nodeGpuFragAmount(node.free_gpus)
			newNodeFragScore = self.nodeGpuFragAmount(node.free_gpus - partial_node_nmu)
			score = newNodeFragScore - nodeFragScore
			node_score_list.append(score)
		# pick the node with the least ∆
		alloc_nodes.append((nodes[node_score_list.index(min(node_score_list))], partial_node_nmu))

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