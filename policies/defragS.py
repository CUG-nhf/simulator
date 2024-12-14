from .policy import Policy
import sys


class DeFragScheduler(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		super(DeFragScheduler, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts)
		self._name = 'defragS'
		self.job_selector = 'sdf'
		self.sqf_min = 0
		self.sqf_max = 0

		for job in self.trace.job_list:
			sqf = job['remain']/job['gpu_num']
			self.sqf_max = max(self.sqf_max, sqf)
			self.sqf_min = min(self.sqf_min, sqf)

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
			if self.job_selector in ['sqf', 'fifo', 'sjf']:
				self.pendJob2()
			elif self.job_selector in ['sdf']:
				self.pendJob1()
			
			self.defragmentation()

			'''3. Log & Result Recorder'''
			if self.time % 10000 == 0:
				self.runtime_log()

			# Sample Cluster State Every Minute
			if self.time % 60 == 0:
				self.seq_recorder()

			self.time += 1
			self.process_running_job()

		self.log_recorder(self._name)
	

	def pendJob2(self):
		que_ls = self.que_list.copy()  # Avoid list.remove() issue
		if self.job_selector == 'fifo':
			que_ls.sort(key=lambda x: x.__getitem__('submit_time'))
		elif self.job_selector == 'sqf':
			que_ls.sort(key=lambda x: x.__getitem__('duration') / x.__getitem__('gpu_num'))
		elif self.job_selector == 'sjf':
			que_ls.sort(key=lambda x: x.__getitem__('duration'))
		for job in que_ls:
			if self.jobPlacer(job):
				job['start_time'] = self.time
				job['end_time'] = job['start_time'] + job['duration']
				job['queue'] = self.time - job['submit_time']
				job['status'] = 'run'
				self.que_list.remove(job)
				self.run_list.append(job)
			else:
				break
		
	def pendJob1(self):
		job, alloc_nodes, score = self.jobSelector()
		while job != None:
			print(f"score: {score}")
			for (node, req_gpu) in alloc_nodes:
				node.allocate_gpu(req_gpu)
				node.add_job(job)
				job['nodes'].append({node.node_name: req_gpu})
			job['start_time'] = self.time
			job['end_time'] = job['start_time'] + job['duration']
			job['queue'] = self.time - job['submit_time']
			job['status'] = 'run'
			self.que_list.remove(job)
			self.run_list.append(job)
			job, alloc_nodes, score = self.jobSelector()


	def jobSelector(self):
		min_score = sys.float_info.max
		min_job = None
		target_node = None

		for job in self.que_list:
			select_flag, alloc_nodes, score = self.nodesSelect(job, True)
			if select_flag:
				if min_job == None or score < min_score:
					min_job, min_score, target_node = job, score, alloc_nodes
			
		return min_job, target_node, min_score
	

	def jobPlacer(self, job):
		vc_free_gpu_num = self._vc.vc_free_gpus()
		job_gpu_num = job['gpu_num']

		# Total Free GPU Check
		if vc_free_gpu_num < job_gpu_num:
			return False

		select_flag, alloc_nodes, _ = self.nodesSelect(job)

		''' Placement '''
		if select_flag:
			for (node, req_gpu) in alloc_nodes:
				node.allocate_gpu(req_gpu)
				node.add_job(job)
				job['nodes'].append({node.node_name: req_gpu})
			return True
		else:
			return False
	
	def nodesSelect(self, job, isJobSelector=False):
		job_gpu_num = job['gpu_num']
		alloc_nodes = []
		complete_node_num = job_gpu_num // 8
		partial_node_nmu = job_gpu_num % 8

		nodes = sorted(self._vc.avail_node_list(),key=lambda x: x.free_gpus, reverse=True)

		'''assign completely idle nodes -- Consolidate'''
		while complete_node_num > 0:
			if len(nodes) > 0 and nodes[0].free_gpus == 8:
				alloc_nodes.append((nodes[0], 8))
				complete_node_num -= 1
				nodes.pop(0)
			else:
				return False, alloc_nodes, sys.float_info.max
			
		if partial_node_nmu == 0:
			return True, alloc_nodes, (job['remain']/job['gpu_num'] - self.sqf_min)/(self.sqf_max - self.sqf_min)

		'''assign partially idle nodes -- DeFragS-consolidate'''
		# 1) Filter out unavailable nodes
		nodes = [node for node in nodes if node.free_gpus >= partial_node_nmu]
		if len(nodes) == 0:
			return False, alloc_nodes, sys.float_info.max
		# Assign Job to node
		target_node = None
		node_score = sys.float_info.max
		for node in nodes:
			node_free_gpus = node.free_gpus
			# 对可用节点进行打分排序，选择分数最小的节点：剩余时间接近，空闲卡数量少
			alpha, beta = 0.1, 0.9
			if not isJobSelector:
				tmp_node_score = alpha*(node_free_gpus-partial_node_nmu)/partial_node_nmu+beta*(abs(node.getLargestReaminTime()-job['remain']))/max(job['remain'], node.getLargestReaminTime())
			else:
				tmp_node_score = self.calculateFitnessScore(node, job, node_free_gpus, partial_node_nmu)
			if target_node == None:
				target_node = node
				node_score = tmp_node_score
			elif tmp_node_score < node_score:
				target_node = node
				node_score = tmp_node_score
		alloc_nodes.append((target_node, partial_node_nmu))

		return True, alloc_nodes, node_score
	
	def calculateFitnessScore(self, node, job, node_free_gpu, job_req_gpu):
		alpha, beta = 0.1, 0.9
		return	alpha*(node_free_gpu-job_req_gpu)/job_req_gpu \
				+ beta*(abs(node.getLargestReaminTime()-job['remain']))/max(job['remain'], node.getLargestReaminTime()) \
				+ (job['remain']/job['gpu_num'] - self.sqf_min) / (self.sqf_max - self.sqf_min)

	def defragmentation(self):
		if self.time - self.last_defrag_time < 5*60:
			return
		if len(self.que_list) > 5 and self._vc.vc_free_gpus() > self.que_list[0]['gpu_num']:
			migrationMap = self._vc.defragmentation()
			self.last_defrag_time = self.time
			for job, source_node, target_node, job_req_gpu in migrationMap:
				print(f'''TIME:{self.time},VC:{self._vc.vc_name}-- {job['jobname']} FROM {source_node.node_name} MIGRATE TO {target_node.node_name} WITH {job_req_gpu} GPUs''')