class Job(dict):
	def __init__(self, series):
		super(Job, self).__init__()
		self.update(series.to_dict())
		# Priority Define by Estimator, Random Means No History Data Found
		self.update({'nodes': [], 'priority': -1, 'random': 0})
	
	def __lt__(self, other):
		return self.__getitem__('gpu_num') < other['gpu_num']

	def set_ckpt_time(self, time):
		self.last_ckpt_time = time

	def get_ckpt_time(self):
		return self.last_ckpt_time
	
	def modify_nodes(self, origin_node_name, new_node_name, job_req_gpu):
		need_new_item = True
		for dict in self.__getitem__('nodes')[:]:  #遍历副本，防止remove后改变遍历顺序
			node_name, _ = next(iter(dict.items()))
			if node_name == origin_node_name:
				self.__getitem__('nodes').remove(dict)
			elif node_name == new_node_name:
				dict[node_name] += job_req_gpu  #  ls[:]是ls的浅拷贝，修改dict可直接修改到原列表
				need_new_item = False
		if need_new_item:
			self.__getitem__('nodes').append({new_node_name: job_req_gpu})
		

class Trace:
	def __init__(self):
		self.job_list = []
	
	def __iter__(self):
		return iter(self.job_list)
	
	def append_job(self, job):
		self.job_list.append(job)

	def job_num(self):
		return len(self.job_list)

	def sort_jobs(self, key):
		self.job_list.sort(key=lambda x: x.__getitem__(key))

	def vc_trace(self, vc_name):
		vc_trace = Trace()
		for job in self.job_list:
			if job['vc'] == vc_name:
				vc_trace.append_job(job)
		vc_trace.sort_jobs('submit_time')
		return vc_trace
