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

	def init_vc_node(self):
		for i in range(self.node_num):
			node = Node(i, self._num_gpus_per_node, self._num_gpus_per_node)
			self.node_list.append(node)

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

	def migrationJob(self, migrationMap):
		for job, source_node, target_node, job_req_gpu in migrationMap:
			# 源节点释放资源
			source_node.release_gpu(job_req_gpu)
			source_node.delete_job(job)

			# 目标节点分配资源
			target_node.allocate_gpu(job_req_gpu)
			target_node.add_job(job)

			# 修改job信息
			for dict in job['nodes']:
				for i, _ in dict.items():
					node = self.node_list[i]
					assert node.node_name == i
					if node == source_node:
						job['nodes'].remove(dict)
			job['nodes'].append({target_node.node_name: job_req_gpu})
	
	def frag_node_list(self):
		# 判断什么样的节点才是碎片节点
		list = []
		for node in self.node_list:
			if 0 < node.free_gpus < 8:
				list.append(node)
		# list = sorted(list, key=lambda x : x.free_gpus, reverse=True) # 降序
		return list
	
	def defragmentation(self):
		# 碎片整理路径：1.选源主机 2.选作业 3.选目标主机 （打分排序）
		# TODO: 作业迁移的代价还没考虑
		# 	  既然作业当成已知，碎片整理时，可以把等待队列里的作业情况考虑进来
		migrationMap = []
		failed_node = None
		loop_times = 0
		while True:
			frag_node_list = self.frag_node_list()
			if failed_node != None and failed_node in frag_node_list:
				frag_node_list.remove(failed_node)
				failed_node = None
			
			loop_times += 1
			if (loop_times > len(frag_node_list)):
				return migrationMap
				
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
					tmp_node_score = 0.1*(node_free_gpus-job_req_gpu)/job_req_gpu+0.9*(abs(node.getLargestReaminTime()-job['remain']))/job['remain']
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
				loop_times = 0
			else:
				failed_node = source_node  
			"""
			TODO：源节点迁出失败，那是其否可以成为目标节点呢？
			例如两个7/8节点，一个2/8节点，7/8的节点作为源节点时，无法为其上的作业找的迁出节点，故迁出失败。但反过来，2/8节点上个的两个1GPU作业可以迁到两个7/8节点上
			还是说由于打分机制，避免了这种可能。例如在上面的情况下，打分使得2/8节点先作为源节点
			"""
				
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
		self.running_jobs.append(job)
	
	def delete_job(self, job):
		for idx, _job in enumerate(self.running_jobs):
			if job == _job:
				self.running_jobs.pop(idx)
				break
		return True

	'''allocate'''
	def allocate_gpu(self, num_gpu):
		if num_gpu > self.free_gpus:
			return False
		else:
			self.free_gpus -= num_gpu
			self.job_num += 1
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
		self.job_num -= 1
		return True

	def release_cpu(self, num_cpu):
		assert self.free_cpus + num_cpu <= self.num_cpus
		self.free_cpus += num_cpu
		return True
