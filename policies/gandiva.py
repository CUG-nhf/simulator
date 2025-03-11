from .policy import Policy
from .placer.stBestFit import SpatioTemporalBestFit

class Gandiva(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		super(Gandiva, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts)
		self._name = 'gandiva'

	def simulate(self):
		prev_index = 0

		while self.end_job_num != self.total_job_num:
			'''1. Check & Release End Jobs'''
			need_defrag = False
			run_ls = self.run_list.copy()  # Avoid list.remove() issue
			for job in run_ls:
				if self.time == job['end_time']:
					job['remain'] = 0
					job['status'] = 'end'
					self.end_job_num += 1
					assert self._vc.release_resource(job['nodes'], job) == True
					self.run_list.remove(job)
					need_defrag = True
			# Defragmentation is triggered at the end of the job
			if need_defrag:
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
				if self._job_placer.place(job):
					job['start_time'] = self.time
					job['end_time'] = job['start_time'] + job['duration']
					job['queue'] = self.time - job['submit_time']
					job['status'] = 'run'
					self.que_list.remove(job)
					self.run_list.append(job)
					need_defrag = True
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
		frag_node_list.sort(key=lambda x: x.free_gpus, reverse=True)

		for idx, source_node in enumerate(frag_node_list.copy()):
			if source_node.free_gpus < 5:
				return
			
			for job, job_req_gpu in source_node.running_jobs.copy():
				# firstly try to migrate multi-nodes job to one node for better locaticity 
				find_tgrt = False
				if len(job['nodes']) > 1:
					for dict in job['nodes']:
						tmp_node_name, _ = next(iter(dict.items()))
						tmp_node = self._vc.get_vc_node(tmp_node_name)
						if tmp_node != source_node and tmp_node.free_gpus >= job_req_gpu:
							self._vc.migrationJob([(job, source_node, tmp_node, job_req_gpu)])
							find_tgrt = True
							break

				if find_tgrt == False:
					# Try to aggregate the scattered jobs into one node, and migrate single-machine job.
					for i in range(len(frag_node_list)-1, idx, -1):
						tmp_node = frag_node_list[i]
						if tmp_node != source_node and tmp_node.free_gpus >= job_req_gpu:
							self._vc.migrationJob([(job, source_node, tmp_node, job_req_gpu)])
							break
