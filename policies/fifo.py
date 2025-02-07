from .policy import Policy
import random


class FirstInFirstOut(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		super(FirstInFirstOut, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts)
		self._name = 'fifo'

		# random.seed(45)
		# error = 0.45
		# for job in self.trace.job_list:
		# 	job['true_duration'] = job['duration']
		# 	job['duration'] = (random.uniform(-error, error) + 1) * job['duration']
		# 	job['remain'] = job['duration']

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
			# NOTE: Sort by submit time -- FIFO
			self.que_list.sort(key=lambda x: x.__getitem__('submit_time'))
			que_ls = self.que_list.copy()  # Avoid list.remove() issue
			for job in que_ls:
				if self._job_placer.place(job):
					job['start_time'] = self.time
					job['end_time'] = job['start_time'] + job['duration'] #  job['true_duration']
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
