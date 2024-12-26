import pandas as pd
import time
from datetime import datetime
import os
import argparse
import multiprocessing
import utils
import cluster
from estimators import NaiveEstimator, LGBEstimator, CombinedEstimator, PhillyEstimator
os.environ['NUMEXPR_MAX_THREADS'] = str(os.cpu_count())

def main(args):
	
	start_time =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	print("start time:", start_time)

	code_start = time.perf_counter()

	'''Logger Setting'''
	log_dir = f'{args.log_dir}/{args.experiment_name}_{args.scheduler}_{args.placer}'
	if not os.path.exists(log_dir):
		os.makedirs(log_dir + '/logfile')
	logger = utils.logger_init(
		file=f'{log_dir}/logfile/{args.scheduler}_{args.placer}')

	'''Infrastructure & Trace Initialization'''
	vc_dict = pd.read_pickle(args.trace_dir+'/vc_dict_homo.pkl')

	# Construct trace DataFrame from cluster_log.csv
	if 'Philly' in args.experiment_name:
		trace_range = ('2017-10-01 00:00:00', '2017-11-30 23:59:00')
		trace_df, start_ts = utils.trace_philly_process(
			args.trace_dir, trace_range, vc_dict, args.mutation)
	else:
		if 'Sept' in args.experiment_name:
			trace_range = ('2020-09-01 00:00:00', '2020-09-26 23:59:00')
			trace_df, start_ts = utils.trace_process(
				args.trace_dir, trace_range, vc_dict)
		elif 'July' in args.experiment_name:
			trace_range = ('2020-07-01 00:00:00', '2020-07-31 23:59:00')
			trace_df, start_ts = utils.trace_process(
				args.trace_dir, trace_range, vc_dict)
		else:
			raise ValueError

	# Construct a Trace object composed of Jobs
	trace = utils.trace_parser(trace_df)

	CLUSTER = cluster.Cluster(
		vc_dict, args.num_gpus_per_node, args.num_cpus_per_node)

	# if 'Philly' in args.experiment_name:
	# 	estimator = PhillyEstimator(args)
	# else:
	# 	estimator = LGBEstimator(args)
	# 	estimator = NaiveEstimator(args)
	# 	estimator = CombinedEstimator(args)
	estimator = None
	'''
	Sweep ON: Run All Scheduler Policies in One Experiment
	Sweep OFF: Run Dedicated Scheduler Policy (Default)
	'''
	if args.sweep:
		process_num = os.cpu_count()
		all_args_list = []
		for policy in utils.get_available_schedulers():
			if policy == 'qssf':
				for i in range(len(vc_dict)):
					all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
										  log_dir, policy, logger, start_ts, estimator))
			else:
				for i in range(len(vc_dict)):
					all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
										  log_dir, policy, logger, start_ts))
	else:
		if args.processes is None:
			process_num = min(len(CLUSTER.vc_list), os.cpu_count())
		else:
			process_num = args.processes
		
		all_args_list = []
		for i in range(len(vc_dict)):
			if args.scheduler == 'qssf':
				all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
									  log_dir, args.scheduler, logger, start_ts, estimator))
			else:
				all_args_list.append((trace, CLUSTER.vc_list[i], args.placer,
									  log_dir, args.scheduler, logger, start_ts))

	with multiprocessing.Pool(processes=process_num) as p:
		results = [p.apply_async(utils.simulate_vc, args_list)
				   for args_list in all_args_list]
		results = [result.get() for result in results]

	if args.sweep:
		for policy in utils.get_available_schedulers():
			utils.cluster_concatenate(
				policy, args.placer, log_dir, vc_dict)
		utils.cluster_analysis('ALL', args.placer, log_dir, vc_dict)
	else:
		utils.cluster_concatenate(
			args.scheduler, args.placer, log_dir, vc_dict)
		utils.cluster_analysis(args.scheduler, args.placer, log_dir, vc_dict)
	
	logger.info(
		f'Execution Time: {round(time.perf_counter() - code_start, 2)}s')
	
	end_time =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	print("start time:", end_time)

	# 计算时间差
	end_time_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
	start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
	time_difference = end_time_dt - start_time_dt

	# 输出时间差
	print("时间差:", time_difference)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Simulator')
	parser.add_argument('-e', '--experiment-name', default='Philly',
						type=str, help='Experiment Name')
	parser.add_argument('-t', '--trace-dir', default='./data/Philly',
						type=str, help='Trace File Directory')
	parser.add_argument('-l', '--log-dir', default='./log',
						type=str, help='Log Directory')

	parser.add_argument('-s', '--scheduler', default='fifo',
						choices=utils.get_available_schedulers(), type=str, help='Scheduler Algorithm')
	parser.add_argument('-p', '--placer', default='consolidate',
						type=str, help='Placer Algorithm') # choices=utils.get_available_placers(),
	
	parser.add_argument('--mutation', action='store_true', default=False,
						help='mutation workload')
	
	parser.add_argument('--sweep', action='store_true', default=False,
						help='Run All Scheduler Policies in One Time')
	parser.add_argument('-j', '--processes', type=int, default=None,
						help=('Number of processes to use in multiprocessing.Pool'
							  '(use as many as available if not specified)'))
	parser.add_argument('--timeout', default=1209600, type=int,
						help='Timeout (in seconds), default 14 days')
	parser.add_argument('--num_gpus_per_node', type=int, default=8,
						help=('Number of GPUs per node'))
	parser.add_argument('--num_cpus_per_node', type=int, default=96,
						help=('Number of CPU cores per node'))

	args = parser.parse_args()
	main(args)