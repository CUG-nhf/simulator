from .policy import Policy
from .placer.consolidate import ConsolidatePlacement
import sys


class DeFragScheduler(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		super(DeFragScheduler, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts)
		self._name = 'defragS'
		splits = self._placement.split('_')
		self._jobSelector = self._placement.split('_')[0]
		if len(splits) > 1:
			self._jobPlacer = self._placement.split('_')[1]
		else:
			self._jobPlacer = None

		self.sqf_min = 0
		self.sqf_max = 0.1

		if self._jobSelector == 'sdf':
			self.calculateFitnessScore = self.calculateFitnessScore_sdf
			for job in self.trace.job_list:
				sqf = job['remain']/job['gpu_num']
				self.sqf_max = max(self.sqf_max, sqf)
				self.sqf_min = min(self.sqf_min, sqf)
		else:
			self.calculateFitnessScore = self.calculateFitnessScore_other

	def simulate(self):
		prev_index = 0

		while self.end_job_num != self.total_job_num:

			need_defrag = False
			'''1. Check & Release End Jobs'''
			run_ls = self.run_list.copy()  # Avoid list.remove() issue
			for job in run_ls:
				if self.time == job['end_time']:
					job['remain'] = 0
					job['status'] = 'end'
					self.end_job_num += 1
					assert self._vc.release_resource(job['nodes'], job) == True
					self.run_list.remove(job)
					need_defrag = True
			if need_defrag:
				self.defragmentation()

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

			# Pen d Job
			if self._jobSelector in ['fifo', 'sjf', 'dynamic']:
				need_defrag =  self.pendJob2()
			elif self._jobSelector in ['sdf']:
				need_defrag = self.pendJob1()
			
			if need_defrag:
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
	
	def calScore(self, job):
		window_size = 5
		if self.time - job['submit_time'] > job['duration']:
			return - (self.time - job['submit_time'] + job['duration'])/job['duration']
		if len(self.gpu_utilization) < window_size or sum(self.gpu_utilization[-window_size:])/window_size < 0.8:
			return job['submit_time']
		else:
			return job['gpu_num']
	def pendJob2(self):
		flag = False
		que_ls = self.que_list.copy()  # Avoid list.remove() issue
		if self._jobSelector == 'fifo':
			que_ls.sort(key=lambda x: x.__getitem__('submit_time'))
		elif self._jobSelector == 'sjf':
			que_ls.sort(key=lambda x: x.__getitem__('duration'))
		elif self._jobSelector == 'dynamic':
			que_ls.sort(key=lambda x: self.calScore(x))
		
		if self._jobPlacer == 'consolidate':
			jobPlacer = ConsolidatePlacement(self._vc).place
		else:
			jobPlacer = self.jobPlacer
		for job in que_ls:
			if jobPlacer(job):
				job['start_time'] = self.time
				job['end_time'] = job['start_time'] + job['duration']
				job['queue'] = self.time - job['submit_time']
				job['status'] = 'run'
				self.que_list.remove(job)
				self.run_list.append(job)
				flag = True
			else:
				break
		return	flag
		
	def pendJob1(self):
		flag = False
		job, alloc_nodes = self.jobSelector()
		while job != None:
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
			job, alloc_nodes = self.jobSelector()
			flag = True
		return flag
 
	def jobSelector(self):
		min_score = sys.float_info.max
		min_job = None
		target_node = None

		for job in self.que_list:
			select_flag, alloc_nodes, score = self.nodesSelect(job)
			if select_flag:
				if min_job == None or score < min_score:
					min_job, min_score, target_node = job, score, alloc_nodes
		print(min_score)
		return min_job, target_node
	
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
	
	def nodesSelect(self, job):
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
			tmp_node_score = self.calculateFitnessScore(node, job, node_free_gpus, partial_node_nmu)
			if target_node == None:
				target_node = node
				node_score = tmp_node_score
			elif tmp_node_score < node_score:
				target_node = node
				node_score = tmp_node_score
		alloc_nodes.append((target_node, partial_node_nmu))

		return True, alloc_nodes, node_score
	
	def calculateFitnessScore_other(self, node, job, node_free_gpu, job_req_gpu):
		alpha, beta = 0.1, 0.9
		return	alpha * (node_free_gpu-job_req_gpu)/job_req_gpu \
				+ beta * (abs(node.getLargestReaminTime()-job['remain']))/max(job['remain'], node.getLargestReaminTime())
		
	def calculateFitnessScore_sdf(self, node, job, node_free_gpu, job_req_gpu):
		alpha, beta, gamma, delta = 0.1, 0.8, 0.1, 1
		return	alpha * (node_free_gpu-job_req_gpu)/node.num_gpus \
				+ beta * (abs(node.getLargestReaminTime()-job['remain']))/max(job['remain'], node.getLargestReaminTime()) \
				+ gamma * (job['remain']/job['gpu_num'] - self.sqf_min) / (self.sqf_max - self.sqf_min) \
				- delta * (self.time - job['submit_time'])/job['duration']

	def defragmentation(self):
		migrationMap = self._vc.defragmentation()
		for job, source_node, target_node, job_req_gpu in migrationMap:
			print(f'''TIME:{self.time},VC:{self._vc.vc_name}-- {job['jobname']} FROM {source_node.node_name} MIGRATE TO {target_node.node_name} WITH {job_req_gpu} GPUs''')
