from .placer.consolidate import ConsolidatePlacement
from .placer.fgd import FragmentationGradientDescent
from .placer.stBestFit import SpatioTemporalBestFit
from .placer.dotProd import DotProdPlacement
from .placer.random import RandomPlacement
from .placer.clustering import ClusteringPlacement
from .placer.worstFit import WorstFitPlacement
import pandas as pd
import os

class Policy:
	def __init__(self, trace, vc, placement, log_dir, logger, start_ts):
		self._placement = placement
		self._vc = vc
		self._vc_name = vc.vc_name
		self._log_dir = log_dir
		self.trace = trace.vc_trace(vc.vc_name)
		self.logger = logger
		self.start_ts = start_ts
		self._jobPopulation = self.calculateJobPopulation() # real Job pupulation for calculating fragmentation ratio
		self._job_placer = self.job_placer()

		self.total_job_num = self.trace.job_num()
		self.que_list = []  # Pending Jobs
		self.run_list = []  # Running Jobs
		self.end_job_num = 0
		self.time = start_ts

		# Time Sequence Recoder
		self.time_list = []
		self.total_gpu_num = []
		self.idle_gpu_num = []
		self.pend_gpu_num = []
		self.run_job_num = []
		self.pend_job_num = []
		self.pend_job_num_less_8 = []
		self.total_node_num = []
		self.consolidate_node_num = []
		self.partial_node_num = []
		self.free_node_num = []  # Node Number with 8 free GPUs
		self.frag_gpu_num = []
		self.gpu_utilization = []
	
	def calculateJobPopulation(self):
		gpu_count_map = {}
		for job in self.trace.job_list:
			gpu_num = job['gpu_num'] % 8
			gpu_num = gpu_num if gpu_num != 0 else 8
			gpu_count_map[gpu_num] = gpu_count_map.get(gpu_num, 0) + 1

		total = sum(gpu_count_map.values())
		normalized_gpu_count_map = {k : v / total for k, v in gpu_count_map.items()}
		return normalized_gpu_count_map
		
	def job_placer(self):
		if 'worstFit' in self._placement:
			return WorstFitPlacement(self._vc)
		if 'clustering' in self._placement:
			return ClusteringPlacement(self._vc)
		if 'dotProd' in self._placement:
			return DotProdPlacement(self._vc)
		if 'random' in self._placement:
			return RandomPlacement(self._vc)
		if 'consolidate' in self._placement:  # actually it is BestFit
			return ConsolidatePlacement(self._vc)
		if 'FGD' in self._placement:
			return FragmentationGradientDescent(self._vc, self._jobPopulation)
		if 'stBestFit' in self._placement:
			return SpatioTemporalBestFit(self._vc)
		return None

	def ckpt_overhead(self, job):
		gpu_num = job.__getitem__('gpu_num')
		if gpu_num == 1:
			return 7
		elif gpu_num <= 8:
			return 30
		else:
			return 60
	def process_running_job(self):
		for job in self.run_list:
			job['remain'] -= 1
	def runtime_log(self):
		self.logger.info(
			f'{self._vc_name} | Time: {int(self.time)} | Total Job: {self.total_job_num} | End job: {self.end_job_num} | Running job: {len(self.run_list)} | Pending job: {len(self.que_list)}')

	'''Simulation Result Recorder'''
	def log_recorder(self, policy_name):
		self.seq_recorder()
		if not os.path.exists(os.path.join(self._log_dir, self._vc_name)):
			os.mkdir(os.path.join(self._log_dir, self._vc_name))

		df = pd.DataFrame(self.trace.job_list)

		if len(df) == 0:
			print('No Job in VC: ', self._vc_name)
			raise NotImplementedError

		df['jct'] = df['end_time'] - df['submit_time']
		# TODO : if consider ckpt overhead, the job['slowdown'] should be changed
		df['slowdown'] = round((df['queue'] + df['duration']) / df['duration'], 2)
		avg_jct = round(df['jct'].mean(), 2)
		avg_que = round(df['queue'].mean(), 2)
		self.logger.info(
			f'{self._vc_name} | Average JCT: {avg_jct} | Average Queue: {avg_que}')

		df.to_csv(
			f'{self._log_dir}/{self._vc_name}/{policy_name}_{self._placement}_{self._vc_name}_log.csv', index=False)
		
		# Time Sequence
		seq_dict = {'time': self.time_list,
					'total_gpu_num': self.total_gpu_num,
					'idle_gpu_num': self.idle_gpu_num,
					'pending_gpu_num': self.pend_gpu_num,
					'running_gpujob_num': self.run_job_num,
					'pending_gpujob_num': self.pend_job_num,
					'pending_job_num_less_8': self.pend_job_num_less_8,
					'total_node_num': self.total_node_num,
					'consolidate_node_num': self.consolidate_node_num,
					'partial_node_num': self.partial_node_num,
					'free_node_num': self.free_node_num,
					'frag_gpu_num': self.frag_gpu_num,
					'gpu_utilization': self.gpu_utilization
					}
		seq = pd.DataFrame(seq_dict)
		seq['fragmentation_ratio'] = (seq['frag_gpu_num']/seq['total_gpu_num']).round(3)
		seq.to_csv(f'{self._log_dir}/{self._vc_name}/{policy_name}_{self._placement}_{self._vc_name}_seq.csv', index=False)
		
	def pend_job_num_small(self):
		job_num = 0
		for job in self.que_list:
			if job.__getitem__('gpu_num') < 8:
				job_num += 1
		return job_num

	def seq_recorder(self):
		self.time_list.append(self.time)
		self.total_gpu_num.append(self._vc.total_gpus)
		self.idle_gpu_num.append(self._vc.vc_free_gpus())
		self.run_job_num.append(len(self.run_list))
		self.pend_job_num.append(len(self.que_list))
		self.pend_job_num_less_8.append(self.pend_job_num_small())
		self.pend_gpu_num.append(sum(job.__getitem__('gpu_num') for job in self.que_list))
		self.total_node_num.append(self._vc.node_num)
		self.consolidate_node_num.append(self._vc.consolidate_node_num())
		self.partial_node_num.append(self._vc.partial_node_num())
		self.free_node_num.append(self._vc.free_node_num())
		self.frag_gpu_num.append(self.get_frag_gpus())
		self.gpu_utilization.append(round((self._vc.total_gpus - self._vc.vc_free_gpus())/self._vc.total_gpus, 3))
	
	"Fragmentation Ratio"
	# Fragmentation refers to the free GPUs of a node whose number of free GPUs is not equal to its total number of GPUs.
	def get_frag_gpus(self):
		frag_gpu_num = 0
		for node in self._vc.node_list:
			if node.free_gpus < node.num_gpus:
				frag_gpu_num += node.free_gpus
		return frag_gpu_num

