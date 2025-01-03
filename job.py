class Job(dict):
	def __init__(self, series):
		super(Job, self).__init__()
		self.update(series.to_dict())
		# Priority Define by Estimator, Random Means No History Data Found
		self.update({'nodes': [], 'priority': -1, 'random': 0})

	def set_ckpt_time(self, time):
		self.last_ckpt_time = time

	def get_ckpt_time(self):
		return self.last_ckpt_time
	
	def get_req_gpu(self, node):
		for dict in self.__getitem__('nodes'):
			node_name, req_gpu = next(iter(dict.items()))
			if node_name == node.node_name:
				return req_gpu


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
