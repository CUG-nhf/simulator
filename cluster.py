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
		self.node_list = []
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
		res = 0
		for node in self.node_list:
			if node.free_gpus == 0:
				res += 1
		return res

	def partial_node_num(self):
		res = 0
		for node in self.node_list:
			if 0 < node.free_gpus < node.num_gpus:
				res += 1
		return res
	
	def free_node_num(self):
		res = 0
		for node in self.node_list:
			if node.free_gpus == node.num_gpus:
				res += 1
		return res
	
	'''DeFragmentation'''
	def migrationJob(self, migrationMap):
		for job, source_node, target_node, job_req_gpu in migrationMap:
			# 源节点释放资源
			source_node.release_gpu(job_req_gpu)
			source_node.delete_job(job)

			# 目标节点分配资源
			target_node.allocate_gpu(job_req_gpu)
			target_node.add_job(job, job_req_gpu)

			job.modify_nodes(source_node.node_name, target_node.node_name, job_req_gpu)
			
			job['ckpt_times'] += 1
	
	def swapJob(self, job_info_1, job_info_2):
		source_node, job, job_req_gpu = job_info_1
		target_node, target_node_job, target_node_job_req_gpu = job_info_2
		
		# 先释放资源
		source_node.release_gpu(job_req_gpu)
		source_node.delete_job(job)
		target_node.release_gpu(target_node_job_req_gpu)
		target_node.delete_job(target_node_job)

		# 再分配资源
		target_node.allocate_gpu(job_req_gpu)
		target_node.add_job(job, job_req_gpu)
		source_node.allocate_gpu(target_node_job_req_gpu)
		source_node.add_job(target_node_job, target_node_job_req_gpu)


		job.modify_nodes(source_node.node_name, target_node.node_name, job_req_gpu)
		target_node_job.modify_nodes(target_node.node_name, source_node.node_name, target_node_job_req_gpu)
		
		job['ckpt_times'] += 1
		target_node_job['ckpt_times'] += 1
				
	def frag_node_list(self):
		# 判断什么样的节点才是碎片节点
		list = []
		for node in self.node_list:
			if 0 < node.free_gpus < 8:
				list.append(node)
		return list

				
class Node:
	def __init__(self, node_name, num_gpus, num_cpus):
		self.node_name = node_name
		self.running_jobs = []
		self.num_gpus = num_gpus
		self.num_cpus = num_cpus
		self.free_gpus = num_gpus
		self.free_cpus = num_cpus
	
	def used_gpu(self):
		return self.num_gpus - self.free_gpus
	
	def getLargestReaminTime(self):
		largest = 0
		max_gpu = 0
		for job, gpu in self.running_jobs:
			if gpu > max_gpu:
				largest = job['remain']
		return largest
	
	def add_job(self, job, job_req_gpu):
		for _job, gpu_num in self.running_jobs.copy():
			if job == _job:
				self.running_jobs.remove((job, gpu_num))
				self.running_jobs.append((job, job_req_gpu+gpu_num))
				return
		self.running_jobs.append((job, job_req_gpu))

	def delete_job(self, job):
		for _job, gpu in self.running_jobs:
			if _job == job:
				self.running_jobs.remove((job, gpu))
				break

	'''allocate'''
	def allocate_gpu(self, num_gpu):
		if num_gpu > self.free_gpus:
			raise ValueError(f"Cannot allocate {num_gpu} GPUs. Only {self.free_gpus} GPUs are available.")
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
	
