import sys
import os
import logging
import datetime
import time
import numpy as np
import pandas as pd
from job import Job, Trace
from policies import ShortestJobFirst, FirstInFirstOut, Gandiva, DeFragScheduler
sys.path.append('..')


def simulate_vc(trace, vc, placement, log_dir, policy, logger, start_ts, *args):
	if policy == 'sjf':
		scheduler = ShortestJobFirst(
			trace, vc, placement, log_dir, logger, start_ts)
	elif policy == 'fifo':
		scheduler = FirstInFirstOut(
			trace, vc, placement, log_dir, logger, start_ts)
	elif policy == 'gandiva':
		scheduler = Gandiva(
			trace, vc, placement, log_dir, logger, start_ts)
	elif policy == 'defragS':
		scheduler = DeFragScheduler(
			trace, vc, placement, log_dir, logger, start_ts)
	scheduler.simulate()
	logger.info(f'Finish {vc.vc_name}')
	return True


def get_available_schedulers():
	return ['fifo', 'gandiva', 'defragS', "sjf"]

def get_available_placers():
	return ['random', 'consolidate', 'FGD', "stBestFit", "dotProd", "clustering", "worstFit",]


def trace_process(dir, date_range, vc_dict):
	start = '2020-04-01 00:00:00'
	df = pd.read_csv(dir+'/cluster_log.csv', parse_dates=['submit_time', 'end_time'], usecols=['job_id', 'user', 'vc', 'gpu_num','cpu_num', 'state', 'submit_time', 'duration','end_time'])
	df.rename(columns={'job_id':'jobname'}, inplace=True)
	
	# Consider gpu jobs only
	df = df[df['gpu_num'] > 0]
	# VC filter
	df = df[df['vc'].isin(vc_dict.keys())]
	for k in list(vc_dict.keys()):
		if k not in df.vc.unique():
			del vc_dict[k]
	
	# Drop jobs with error GPUs
	df = df.loc[df['gpu_num'] <= df['vc'].map(vc_dict) * 8]

	df = df[df['submit_time'] >= pd.Timestamp(start)]
	df['submit_time'] = df['submit_time'].apply(
		lambda x: int(datetime.datetime.timestamp(pd.Timestamp(x))))

	# Normalizing
	df['submit_time'] = df['submit_time'] - df.iloc[0]['submit_time']

	df['remain'] = df['duration']
	df[['start_time', 'end_time']] = sys.maxsize
	df[['ckpt_times', 'queue', 'jct']] = 0
	df['status'] = None

	# Slicing simulation part
	begin = (pd.Timestamp(date_range[0])-pd.Timestamp(start)).total_seconds()
	end = (pd.Timestamp(date_range[1])-pd.Timestamp(start)).total_seconds()
	df = df[(df['submit_time'] >= begin) & (df['submit_time'] <= end)]
	
	# Normalize to Zero
	df.sort_values(by='submit_time', inplace=True)
	df.reset_index(inplace=True, drop=True)

	# Drop VCs with no jobs
	for vc in list(vc_dict.keys()).copy():
		if (df[df['vc'] == vc].shape[0] == 0):
			del vc_dict[vc]
			
	return df, begin


def trace_philly_process(dir, date_range, vc_dict):
	start = '2017-10-01 00:00:00'
	df = pd.read_csv(dir+'/cluster_log.csv', parse_dates=['submit_time', 'end_time'], converters={'vc': str},
				  usecols=['user', 'vc', 'jobname', 'gpu_num', 'state', 'submit_time', 'duration', 'end_time'])
	# Consider gpu jobs only
	df = df[df['gpu_num'] > 0]
	# only 3 jobs are deleted
	df = df[~df['gpu_num'].isin([6, 7])]
	
	df = df[df['vc'].isin(vc_dict.keys())]
	
	df = df[df['submit_time'] >= pd.Timestamp(start)]
	df['submit_time'] = df['submit_time'].apply(
		lambda x: int(datetime.datetime.timestamp(pd.Timestamp(x))))

	df['state'] = df['state'].replace('Pass', 'COMPLETED')
	df['state'] = df['state'].replace('Failed', 'FAILED')
	df['state'] = df['state'].replace('Killed', 'CANCELLED')
	
	# Normalizing
	df['submit_time'] = df['submit_time'] - df.iloc[0]['submit_time']

	df['remain'] = df['duration']
	df[['start_time', 'end_time']] = sys.maxsize
	df[['ckpt_times', 'queue', 'jct']] = 0
	df['status'] = None

	# Slicing simulation part
	begin = (pd.Timestamp(date_range[0])-pd.Timestamp(start)).total_seconds()
	end = (pd.Timestamp(date_range[1])-pd.Timestamp(start)).total_seconds()
	df = df[(df['submit_time'] >= begin) & (df['submit_time'] <= end)]

	df.sort_values(by='submit_time', inplace=True)
	df.reset_index(inplace=True, drop=True)
	
	return df, begin

def trace_ali20_process(dir):
	df = pd.read_csv(dir+'/cluster_log.csv')
	df.drop(columns=['inst_id', 'user',], inplace=True)
	df.rename(columns={'job_name':'jobname'}, inplace=True)
	df.rename(columns={'start_time':'submit_time'}, inplace=True)

	df['remain'] = df['duration']
	df[['start_time', 'end_time']] = sys.maxsize
	df[['ckpt_times', 'queue', 'jct']] = 0
	df['status'] = None
	df['vc'] = 'ali20'

	df.sort_values(by='submit_time', inplace=True)
	df.reset_index(inplace=True, drop=True)

	return df, 0


def trace_parser(df):
	trace = Trace()

	for _, series in df.iterrows():
		trace.append_job(Job(series))
	trace.sort_jobs('submit_time')
	return trace


def logger_init(file):
	os.environ['TZ'] = 'Asia/Shanghai'
	time.tzset() 
	logger = logging.getLogger()
	handler_file = logging.FileHandler(f'{file}.log', 'w')

	logger.setLevel(logging.INFO)
	handler_file.setLevel(logging.INFO)

	formatter = logging.Formatter(
		'%(asctime)s | %(processName)s | %(message)s', datefmt='%Y %b %d %H:%M:%S')
	handler_file.setFormatter(formatter)

	logger.addHandler(handler_file)

	return logger


def cluster_concatenate(policy, placer, log_dir, vc_dict):
	prefix = f'{policy}_{placer}'
	if not os.path.exists(log_dir+'/all'):
		os.mkdir(log_dir+'/all')

	vcs = list(vc_dict.keys())
	'''Log'''
	cluster_log = pd.DataFrame()
	for vc in vcs:
		vc_log = pd.read_csv(f'{log_dir}/{vc}/{prefix}_{vc}_log.csv')
		cluster_log = pd.concat([cluster_log, vc_log])
	cluster_log.sort_values(by='submit_time', inplace=True)
	cluster_log.to_csv(f'{log_dir}/all/{prefix}_all_log.csv', index=False)

	'''Seq'''
	cluster_seq = pd.DataFrame()
	add_list = ['total_gpu_num', 'idle_gpu_num', 'pending_gpu_num', 
			 	'running_gpujob_num', 'pending_gpujob_num', 'pending_job_num_less_8', 
				'total_node_num', 'consolidate_node_num', 'partial_node_num', 'free_node_num',
				'frag_gpu_num']
	for vc in vcs:
		vc_seq = pd.read_csv(f'{log_dir}/{vc}/{prefix}_{vc}_seq.csv')
		if len(cluster_seq) == 0:
			cluster_seq = vc_seq
			continue
		cluster_seq[add_list] = cluster_seq[add_list] + vc_seq[add_list]
		cluster_seq.dropna(inplace=True)
		cluster_seq = cluster_seq.astype(int)

	cluster_seq['fragmentation_ratio'] = (cluster_seq['frag_gpu_num'] / cluster_seq['total_gpu_num']).round(3)
	cluster_seq['gpu_utilization'] = ((cluster_seq['total_gpu_num'] - cluster_seq['idle_gpu_num'])/cluster_seq['total_gpu_num']).round(3)
	cluster_seq.to_csv(f'{log_dir}/all/{prefix}_all_seq.csv', index=False)


def cluster_analysis(scheduler, placer, log_dir, vc_dict):
	'''Generate Algorithm Comparsion CSV'''
	# ignore_warm_up = start_ts + 7*24*3600

	prefix_list = []
	if scheduler == 'ALL':
		for i in get_available_schedulers():
			prefix = f'{i}_{placer}'
			prefix_list.append(prefix)
	else :
		prefix = f'{scheduler}_{placer}'
		prefix_list.append(prefix)

	vcs = list(vc_dict.keys())
	vcs.append('all')

	
	jct_avg = pd.DataFrame()
	que_avg = pd.DataFrame()
	for prefix in prefix_list:
		for vc in vcs:
			vc_log = pd.read_csv(f'{log_dir}/{vc}/{prefix}_{vc}_log.csv')
			# vc_log = vc_log[vc_log['submit_time'] > ignore_warm_up]
			jct_avg.at[vc, prefix] = vc_log['jct'].mean()
			que_avg.at[vc, prefix] = vc_log['queue'].mean()

	jct_avg = jct_avg.astype(int)
	que_avg = que_avg.astype(int)
	jct_avg.to_csv(f'{log_dir}/jct_avg.csv')
	que_avg.to_csv(f'{log_dir}/que_avg.csv')

if __name__ == '__main__' :
	vc_dict = pd.read_pickle('./data/Philly' + '/vc_dict_homo.pkl')
	cluster_concatenate("defragS", "sdf_PPT65", "/data/nihaifeng/log/test/Philly_defragS_sdf_PPT65", vc_dict)
	cluster_analysis("defragS", "sdf_PPT65", "/data/nihaifeng/log/test/Philly_defragS_sdf_PPT65", vc_dict)