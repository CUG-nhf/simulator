from .policy import Policy
import queue
import sys
import random

class DeFragScheduler(Policy):
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		super(DeFragScheduler, self).__init__(
			trace, vc, placement, log_dir, logger, start_ts)
		self._name = 'defragS'

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

			# Pend Job
			que_ls = self.que_list.copy()  # Avoid list.remove() issue
			que_ls.sort(key=lambda x: x.__getitem__('submit_time'))
			for job in que_ls:
				if self._job_placer.place(job):
					job['start_time'] = self.time
					job['end_time'] = job['start_time'] + job['duration'] # job['true_duration']
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
	def defragmentation(self):
			# 碎片整理路径：1.选源主机 2.选作业 3.选目标主机 （打分排序）
			frag_node_list = self._vc.frag_node_list()
			
			MAX_MIGRATION_DEEP = 1
			while MAX_MIGRATION_DEEP > 0 and len(frag_node_list) > 1:
				MAX_MIGRATION_DEEP -= 1

				# 1. sorted source nodes
				frag_node_list = sorted(frag_node_list, key=lambda x: (x.used_gpu() / len(x.running_jobs), len(x.running_jobs)))
				for source_node in frag_node_list[:]:  # Avoid list.remove() issue
					if source_node not in frag_node_list:
						continue
					
					# 2.选待迁移作业：优先迁移大作业，防止小作业在两个节点之间来回迁移
					# migration_jobs = sorted(source_node.running_jobs, key=lambda job: job[1], reverse=True)
					# migration_jobs = sorted(source_node.running_jobs, key=lambda job: job[1])
					migration_jobs = queue.PriorityQueue()
					for job, job_req_gpu in source_node.running_jobs:
						migration_jobs.put((job_req_gpu, job))
					# 3.选目标节点 
					# for job, job_req_gpu in migration_jobs:
					while not migration_jobs.empty():
						job_req_gpu, job = migration_jobs.get(migration_jobs)

						tmp = [node for node in frag_node_list if node != source_node]
						select_flag, target_nodes = self._job_placer.nodeSelect(job, job_req_gpu, tmp)
						if select_flag:
							self._vc.migrationJob([(job, source_node, target_nodes[0][0], job_req_gpu)])
							if target_nodes[0][0].free_gpus == 0:
								frag_node_list.remove(target_nodes[0][0])
							if source_node.free_gpus == source_node.num_gpus:
								frag_node_list.remove(source_node)
						else:
							# induced migration
							# 从后往前frag_node_list， 先看踢一个小作业能凑整不
							swap_option = None
							for target_node in reversed(frag_node_list):
								if swap_option==None and target_node != source_node:
									target_node_jobs = sorted(target_node.running_jobs, key=lambda x: x[1]) # 升序
									for target_node_job, target_node_job_req_gpu in target_node_jobs:
										if target_node_job_req_gpu < job_req_gpu and job_req_gpu - target_node_job_req_gpu == target_node.free_gpus:
											swap_option = (target_node, target_node_job, target_node_job_req_gpu)
											break
							# 和一个大作业交换，使得target_node的碎片变大
							for target_node in reversed(frag_node_list):
								if swap_option==None and target_node != source_node:
									target_node_jobs = sorted(target_node.running_jobs, key=lambda x: x[1], reverse=True) # 降序
									for target_node_job, target_node_job_req_gpu in target_node_jobs:
										if target_node_job_req_gpu > job_req_gpu and source_node.free_gpus + job_req_gpu >= target_node_job_req_gpu: 
											swap_option = (target_node, target_node_job, target_node_job_req_gpu)
											break
							if swap_option != None:
								self._vc.swapJob((source_node, job, job_req_gpu), swap_option)
								migration_jobs.put((target_node_job_req_gpu, target_node_job))
								if swap_option[0].free_gpus == 0:
									frag_node_list.remove(swap_option[0])
								if source_node.free_gpus == source_node.num_gpus:
									frag_node_list.remove(source_node)
							else:
								break
							
								