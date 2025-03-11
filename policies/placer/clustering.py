class ClusteringPlacement:
	def __init__(self, vc):
		self.name = 'clustering'
		self.vc = vc
	def nodeSelect(self, job, job_gpu_num, node_list):
		'''
		gandiva: nodes are put into class(1-GPU, 2-GPU, 4-GPU)
		'''
		alloc_nodes = []
		complete_node_num = job_gpu_num // 8
		partial_node_num = job_gpu_num % 8

		'''assign completely idle nodes -- Consolidate'''
		avail_nodes = sorted(node_list, key=lambda x: x.free_gpus, reverse=True)
		while complete_node_num > 0:
			if len(avail_nodes) > 0 and avail_nodes[0].free_gpus == 8:
				alloc_nodes.append((avail_nodes[0], 8))
				# if node[0] is already in node_g, remove it
				for _, node_list in self.vc.node_g.items():
					if avail_nodes[0] in node_list:
						node_list.remove(avail_nodes[0])
				complete_node_num -= 1
				avail_nodes.pop(0)
			else:
				return False, alloc_nodes
			
		if partial_node_num == 0:
			return True, alloc_nodes

		'''assign partially idle nodes -- Gandiva'''
		map_partill_node_num = {
			1: 1,
			2: 2,
			3: 1,
			4: 4,
			5: 1,
			6: 2,
			7: 1,
		}
		assert map_partill_node_num[partial_node_num] in self.vc.node_g
		node_g_list = self.vc.node_g[map_partill_node_num[partial_node_num]]

		# Same affinity with free GPUs
		if len(node_g_list) > 0:
			# return minLoadNode
			node = node_g_list[0]
			if node.free_gpus >= partial_node_num:
				alloc_nodes.append((node, partial_node_num))
				node_g_list.sort(key=lambda x:x.used_gpu())
				return True, alloc_nodes
			
		# Unallocated GPU servers
		if len(avail_nodes) > 0 and avail_nodes[0].free_gpus == 8:
			alloc_nodes.append((avail_nodes[0], partial_node_num))
			# if node[0] is already in node_g, remove it
			for _, node_list in self.vc.node_g.items():
				if avail_nodes[0] in node_list:
					node_list.remove(avail_nodes[0])
			node_g_list.append(avail_nodes[0])
			node_g_list.sort(key=lambda x:x.used_gpu())
			return True, alloc_nodes

		# relax affinity constraint, this is Gandiva's original approach, but it is not used in our experiment

		# for node in avail_nodes:
		# 	if node.free_gpus < partial_node_num:
		# 		alloc_nodes.append((node, node.free_gpus))
		# 		partial_node_num -= node.free_gpus
		# 	else:
		# 		alloc_nodes.append((node, partial_node_num))
		# 		return True, alloc_nodes
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
