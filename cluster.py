class Cluster:
	def __init__(self, vc_dict, num_gpus_per_node, num_cpus_per_node):
		self._vc_dict = vc_dict
		self._num_gpus_per_node = num_gpus_per_node
		self._num_cpus_per_node = num_cpus_per_node
		self.vc_num = len(vc_dict)
		self.node_num = sum(vc_dict.values())
		self.vc_list = []
		self.init_cluster_vc()
		self.total_gpus = sum(vc.total_gpus for vc in self.vc_list)
		self.total_cpus = sum(vc.total_cpus for vc in self.vc_list)

	def init_cluster_vc(self):
		for k, v in self._vc_dict.items():
			vc = VC(k, v, self._num_gpus_per_node, self._num_cpus_per_node)
			self.vc_list.append(vc)

	def cluster_free_gpus(self):
		return sum(vc.vc_free_gpus() for vc in self.vc_list)

	def cluster_free_cpus(self):
		return sum(vc.vc_free_cpus() for vc in self.vc_list)


class VC:
	def __init__(self, vc_name, node_num, num_gpus_per_node, num_cpus_per_node):
		self.vc_name = vc_name
		self.node_num = node_num
		self._num_gpus_per_node = num_gpus_per_node
		self._num_cpus_per_node = num_cpus_per_node
		self.node_list = []
		self.init_vc_node()
		self.total_gpus = num_gpus_per_node * node_num
		self.total_cpus = num_cpus_per_node * node_num

		# for Gandiva
		self.node_g1 = []
		self.node_g2 = []
		self.node_g4 = []
		self.node_g = {1:self.node_g1, 2:self.node_g2, 4:self.node_g4}

	def init_vc_node(self):
		for i in range(self.node_num):
			node = Node(i, self._num_gpus_per_node, self._num_gpus_per_node)
			self.node_list.append(node)
	
	def get_vc_node(self, node_name):
		assert 0 <= node_name < self.node_num
		return self.node_list[node_name]

	def vc_free_gpus(self):
		return sum(node.free_gpus for node in self.node_list)

	def vc_free_cpus(self):
		return sum(node.free_cpus for node in self.node_list)

	def avail_node_list(self):
		avail_node_list = []
		for node in self.node_list:
			if node.free_gpus > 0:
				avail_node_list.append(node)
		return avail_node_list
		
	def release_resource(self, nodes_list, job):
		for dict in nodes_list:
			for i, gpu_num in dict.items():
				node = self.node_list[i]
				assert node.node_name == i
				node.release_gpu(gpu_num)
				node.delete_job(job)
		return True

	def consolidate_node_num(self):
		list = []
		for node in self.node_list:
			if node.job_num == 1:
				list.append(node)
		return len(list)

	def shared_node_num(self):
		list = []
		for node in self.node_list:
			if node.job_num > 1:
				list.append(node)
		return len(list)
	
	'''DeFragmentation'''

	def migrationJob(self, migrationMap):
		for job, source_node, target_node, job_req_gpu in migrationMap:
			# 源节点释放资源
			source_node.release_gpu(job_req_gpu) == True
			source_node.delete_job(job)

			# 目标节点分配资源
			target_node.allocate_gpu(job_req_gpu) == True
			target_node.add_job(job)

			need_new_item = True
			for dict in job['nodes'][:]:  # 遍历副本，防止remove后改变遍历顺序
				node_name, _ = next(iter(dict.items()))
				if node_name == source_node.node_name:
					job['nodes'].remove(dict)
				elif node_name == target_node.node_name:
					dict[node_name] += job_req_gpu
					need_new_item = False
			if need_new_item:
				job['nodes'].append({target_node.node_name: job_req_gpu})
				
	
	def frag_node_list(self):
		# 判断什么样的节点才是碎片节点
		list = []
		for node in self.node_list:
			if 0 < node.free_gpus < 8:
				list.append(node)
		list = sorted(list, key=lambda x : x.free_gpus, reverse=True) # 降序
		return list

	def defragmentation(self, DeFragScheduler):
		# 碎片整理路径：1.选源主机 2.选作业 3.选目标主机 （打分排序）
		migrationMap = []
		while True:
			frag_node_list = self.frag_node_list()
			if len(frag_node_list) < 2:
				return migrationMap
			
			# 1.选源节点: 作业少，剩余时间长
			source_node = None
			node_score = 0
			for node in frag_node_list:
				if source_node == None and node.calculate_node_score() > 0:
					source_node = node
					node_score = node.calculate_node_score()
				elif node.calculate_node_score() > node_score:
					source_node = node
					node_score = node.calculate_node_score()
			
			if source_node == None:
				return migrationMap
			
			# 2.选待迁移作业，暂定全部迁出
			migrationJob = []
			for job in source_node.running_jobs:
				for dict in job['nodes']:
					for i, gpu_num in dict.items():
						node = self.node_list[i]
						assert node.node_name == i
						if node == source_node:
							migrationJob.append((job, gpu_num))
			
			# 3.选目标节点
			tmp_mig_map = []
			for job, job_req_gpu in migrationJob:
				target_node = None
				node_score = 0
				for node in frag_node_list:
					node_free_gpus = node.free_gpus
					for _, _, toNode, gpus in tmp_mig_map:
						if toNode == node:
							node_free_gpus -= gpus
					if  node_free_gpus < job_req_gpu or node == source_node:
						continue
					# 对可用节点进行打分排序，选择分数最小的节点：剩余时间接近，空闲卡数量少
					tmp_node_score = DeFragScheduler.calculateFitnessScore(node, job, node_free_gpus, job_req_gpu)
					if target_node == None:
						target_node = node
						node_score = tmp_node_score
					elif tmp_node_score < node_score:
						target_node = node
						node_score = tmp_node_score
				
				# 如果没找到目标节点，则说明无处可迁
				if target_node == None: 	# TODO:目前的找目标节点是找consolidate的，后面可以考虑加入找多节点的
					break
				tmp_mig_map.append((job, source_node, target_node, job_req_gpu))

			if len(tmp_mig_map) == len(migrationJob):
				self.migrationJob(tmp_mig_map)
				migrationMap += tmp_mig_map
			else:
				return migrationMap
				
class Node:
	def __init__(self, node_name, num_gpus, num_cpus):
		self.node_name = node_name
		self.job_num = 0
		self.running_jobs = []
		self.num_gpus = num_gpus
		self.num_cpus = num_cpus
		self.free_gpus = num_gpus
		self.free_cpus = num_cpus
	
	def used_gpu(self):
		return self.num_gpus - self.free_gpus
	
	def getLargestReaminTime(self):
		largest = 0
		for job in self.running_jobs:
			if job['remain'] > largest:
				largest = job['remain']
		return largest
	
	def calculate_node_score(self):
		# TODO: 调研GPU作业迁移，是作业数量影响大，还是卡数影响大
		remain_time = 0
		total_time = 0
		for job in self.running_jobs:
			remain_time += job['remain']
			total_time += job['duration']
		return remain_time / total_time - self.used_gpu() / self.num_gpus  # 剩余运行时间越大越好，已用GPU越少越好
	
	def add_job(self, job):
		if job not in self.running_jobs:
			self.running_jobs.append(job)
			self.job_num += 1

	def delete_job(self, job):
		if job in self.running_jobs:
			self.running_jobs.remove(job)
			self.job_num -= 1

	'''allocate'''
	def allocate_gpu(self, num_gpu):
		if num_gpu > self.free_gpus:
			return False
		else:
			self.free_gpus -= num_gpu
			return True

	def allocate_cpu(self, num_cpu):
		if num_cpu > self.free_cpus:
			return False
		else:
			self.free_cpus -= num_cpu
			return True

	'''release'''
	def release_gpu(self, num_gpu):
		assert self.free_gpus + num_gpu <= self.num_gpus
		self.free_gpus += num_gpu
		return True

	def release_cpu(self, num_cpu):
		assert self.free_cpus + num_cpu <= self.num_cpus
		self.free_cpus += num_cpu
		return True
	
