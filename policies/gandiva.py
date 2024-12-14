from .policy import Policy


class Gandiva(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts, deFrag):
		super(Gandiva, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts, deFrag)
		self._name = 'gandiva'


	def simulate(self):
		prev_index = 0

		while self.end_job_num != self.total_job_num:
			'''1. Check & Release End Jobs'''
			run_ls = self.run_list.copy()  # Avoid list.remove() issue
			for job in run_ls:
				if self.time == job['end_time']:
					job['remain'] = 0
					job['status'] = 'end'
					self.end_job_num += 1
					assert self._vc.release_resource(job['nodes'], job) == True
					self.run_list.remove(job)
					# Defragmentation is triggered at the end of the job
					self.gandiva_job_migration()
					
			'''2. Allocate New / Pending Jobs'''
			# New Job
			for idx in range(prev_index, self.total_job_num):
				job = self.trace.job_list[idx]
				if job['submit_time'] == self.time:
					job['status'] = 'pend'
					self.que_list.append(job)
					prev_index = idx
				elif job['submit_time'] > self.time:
					break
			# Pend Job
			# NOTE: Sort by submit time -- FIFO
			self.que_list.sort(key=lambda x: x.__getitem__('submit_time'))
			que_ls = self.que_list.copy()  # Avoid list.remove() issue
			for job in que_ls:
				if self.gandiva_placement(job):
					job['start_time'] = self.time
					job['end_time'] = job['start_time'] + job['duration']
					job['queue'] = self.time - job['submit_time']
					job['status'] = 'run'
					self.que_list.remove(job)
					self.run_list.append(job)
				else:
					break

			'''3. Log & Result Recorder'''
			if self.time % 10000 == 0:
				self.runtime_log()

			# Sample Cluster State Every Minute
			if self.time % 60 == 0:
				self.seq_recorder()

			self.time += 1
			self.process_running_job()

		self.log_recorder(self._name)


	def gandiva_job_migration(self):
		'''
		In Gandiva, we use migration to improve locality whenever a job departs 
		and also as a background process to “defrag” the cluster.
		'''
		frag_node_list = self._vc.frag_node_list()

		for source_node in frag_node_list:
			if source_node.free_gpus < 5:
				return
			
			migrationMap = []
			target_nodes = []
			for job in source_node.running_jobs:
				find_tgrt = False
				# firstly try to migrate multi-nodes job to one node for better locaticity 
				if len(job['nodes']) > 1:
					job_req_gpu = job.get_req_gpu(source_node)
					for dict in job['nodes']:
						tmp_node_name, _ = next(iter(dict.items()))
						tmp_node = self._vc.get_vc_node(tmp_node_name)
						# 如果tmp_node之前被选为目标节点，则对应减去tmp_node的free_gpus
						tmp_node_free_gpus = tmp_node.free_gpus
						if tmp_node in target_nodes:
							for _, _, t_node, g in migrationMap:
								if t_node == tmp_node:
									tmp_node_free_gpus -= g
						if tmp_node != source_node and tmp_node_free_gpus >= job_req_gpu:
							target_nodes.append(tmp_node)
							migrationMap.append((job, source_node, tmp_node, job_req_gpu))
							find_tgrt = True
							break
				# Try to aggregate the scattered jobs into one node, and migrate single-machine job.
				if not find_tgrt:
					job_req_gpu = job['gpu_num']
					for i in range(len(frag_node_list)-1, -1, -1):
						tmp_node = frag_node_list[i]
						# 如果tmp_node之前被选为目标节点，则对应减去tmp_node的free_gpus
						tmp_node_free_gpus = tmp_node.free_gpus
						if tmp_node in target_nodes:
							for _, _, t_node, g in migrationMap:
								if t_node == tmp_node:
									tmp_node_free_gpus -= g
						if tmp_node != source_node and tmp_node_free_gpus >= job_req_gpu:
							target_nodes.append(tmp_node)
							for dict in job['nodes']:
								node_name, req_gpu = next(iter(dict.items()))
								migrationMap.append((job, self._vc.get_vc_node(node_name), tmp_node, req_gpu))
							break

			for j, sn, tn, g in migrationMap:
				print(f'''TIME:{self.time},VC:{self._vc.vc_name}-- {j['jobname']} FROM {sn.node_name} MIGRATE TO {tn.node_name} WITH {g} GPUs''')
				# migrate jobs with finding target node
			self._vc.migrationJob(migrationMap)

			
	def gandiva_placement(self, job):
		vc_free_gpu_num = self._vc.vc_free_gpus()
		job_gpu_num = job['gpu_num']

		# Total Free GPU Check
		if vc_free_gpu_num < job_gpu_num:
			return False

		if self._vc._num_gpus_per_node != 8:
			raise NotImplementedError

		select_flag, alloc_nodes = self.gandivaSelect(job_gpu_num)

		''' Placement '''
		if select_flag:
			for (node, req_gpu) in alloc_nodes:
				node.allocate_gpu(req_gpu)
				node.add_job(job)
				job['nodes'].append({node.node_name: req_gpu})
			return True
		else:
			return False
		

	def gandivaSelect(self, job_gpu_num):
		'''
		gandiva: nodes are put into class(1-GPU, 2-GPU, 4-GPU)
		'''
		alloc_nodes = []
		complete_node_num = job_gpu_num // 8
		partial_node_num = job_gpu_num % 8

		'''assign completely idle nodes -- Consolidate'''
		avail_nodes = sorted(self._vc.avail_node_list(),key=lambda x: x.free_gpus, reverse=True)
		while complete_node_num > 0:
			if len(avail_nodes) > 0 and avail_nodes[0].free_gpus == 8:
				alloc_nodes.append((avail_nodes[0], 8))
				# if node[0] is already in node_g, remove it
				for _, node_list in self._vc.node_g.items():
					if avail_nodes[0] in node_list:
						node_list.remove(avail_nodes[0])
				complete_node_num -= 1
				avail_nodes.pop(0)
			else:
				return False, alloc_nodes
			
		if partial_node_num == 0:
			return True, alloc_nodes

		'''assign partially idle nodes -- Gandiva'''
		assert partial_node_num in self._vc.node_g
		node_g_list = self._vc.node_g[partial_node_num]

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
			for _, node_list in self._vc.node_g.items():
				if avail_nodes[0] in node_list:
					node_list.remove(avail_nodes[0])
			node_g_list.append(avail_nodes[0])
			node_g_list.sort(key=lambda x:x.used_gpu())
			return True, alloc_nodes

		# relax affinity constraint
		for node in avail_nodes:
			if node.free_gpus < partial_node_num:
				alloc_nodes.append((node, node.free_gpus))
				partial_node_num -= node.free_gpus
			else:
				alloc_nodes.append((node, partial_node_num))
				return True, alloc_nodes
		return False, alloc_nodes

