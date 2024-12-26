import pandas as pd
import sys
import os
import multiprocessing
import utils
from policies import OPT
import cluster
os.environ['NUMEXPR_MAX_THREADS'] = str(os.cpu_count())

sys.set_int_max_str_digits(5000000)


def simulate_vc(trace, vc, placement, log_dir, policy, logger, start_ts, *args):
	policy = OPT(trace, vc, placement, log_dir, logger, start_ts)
	return policy.simulate()


trace_dir = '/data/nihaifeng/code/HeliosArtifact/simulator/data/Philly'
vc_dict = pd.read_pickle(trace_dir+'/vc_dict_homo.pkl')

trace_range = ('2017-10-01 00:00:00', '2017-11-30 23:59:00')
trace_df, start_ts = utils.trace_philly_process(trace_dir, trace_range, vc_dict, need_mutation=True)

trace = utils.trace_parser(trace_df)
CLUSTER = cluster.Cluster(vc_dict, 8,  96)

process_num = os.cpu_count()
all_args_list = []
for i in range(len(vc_dict)):
	all_args_list.append((trace, CLUSTER.vc_list[i], 'consolidate', None, 'opt', None, start_ts, None))

with multiprocessing.Pool(processes=process_num) as p:
	results = [p.apply_async(simulate_vc, args_list) for args_list in all_args_list]
	results = [result.get() for result in results]

print("results: ")
print(results)

print(f"average Queue: {sum(results) / len(results)}" )
