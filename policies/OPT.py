from .policy import Policy
import sys
import math
from datetime import datetime
import itertools

sys.set_int_max_str_digits(5000000)

class OPT(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		super(OPT, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts)
		self._name = 'opt'

		self.total_perm = math.factorial(len(self.trace.job_list))

	def clear(self):
		self.que_list = []
		self.run_list = []
		self.end_job_num = 0
		self.time = self.start_ts

		for job in self.trace.job_list:
			job['nodes'] = []
		
		self._vc.init_vc_node()

	def simulate(self):
		min_avg_Queue_time = sys.float_info.max

		# 生成并遍历所有全排列
		current_perm = 0
		for jobs in itertools.permutations(self.trace.job_list):
			current_perm += 1
			self.clear()
			self.que_list = list(jobs)
			queue_time = []

			while self.end_job_num != self.total_job_num:
				if len(queue_time) > 0 and sum(queue_time) / len(queue_time) > min_avg_Queue_time:
					break

				#  End Jobs
				run_ls = self.run_list.copy()  # Avoid list.remove() issue
				for job in run_ls:
					if self.time == job['end_time']:
						self.end_job_num += 1
						assert self._vc.release_resource(job['nodes'], job) == True
						self.run_list.remove(job)
					
				# Pend Job
				que_ls = self.que_list.copy()  # Avoid list.remove() issue
				for job in que_ls:
					if self.time < job['submit_time']:
						break
					if self.job_placer(job):
						job['end_time'] = self.time + job['duration']
						queue_time.append(self.time - job['submit_time'])
						self.que_list.remove(job)
						self.run_list.append(job)
					else:
						break

				self.time += 1
			
			avg_Queue_time = sum(queue_time) / len(queue_time)
			if avg_Queue_time < min_avg_Queue_time:
				min_avg_Queue_time = avg_Queue_time
				print(f"Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} VC: {self._vc_name} OPT: {min_avg_Queue_time} 进度：{current_perm/self.total_perm:.5%}")
		
		return min_avg_Queue_time
			

