import json
import os
import pandas as pd
from network.graph import Graph
from network.path_finder import PathFinder

# import 4 strategy schedulers
from network.te_scheduler import TEScheduler
from network.a_star_scheduler import AStarScheduler
from network.pda_scheduler import PDAScheduler
from network.greedy_scheduler import GreedyScheduler

def compare_schedulers(topology_file: str, workload_file: str) -> None:

    # load file
    topology_df = pd.read_csv(topology_file)
    with open(workload_file, 'r') as f:
        workload_data = json.load(f)
    workload_name = os.path.splitext(os.path.basename(workload_file))[0]
    result_dir = os.path.join("result", workload_name)
    os.makedirs(result_dir, exist_ok=True)

    network = Graph.from_dataframe(topology_df)
    path_finder = PathFinder(network)

    result_dir = os.path.join("result", workload_name)
    os.makedirs(result_dir, exist_ok=True)

    # 策略 1: Traffic Engineering (TE)
    te_scheduler = TEScheduler(network, path_finder)
    for job in workload_data:
        te_scheduler.schedule_job(job)  # 依次调度每个任务
    te_total_bw = te_scheduler.get_total_peak_bandwidth()
    print(f"TE: {te_total_bw:.2f}") # Gbps

    te_file = os.path.join(result_dir, "te_result.json")
    with open(te_file, 'w') as f:
        json.dump(te_scheduler.format_results(), f, indent=2)

    # 策略 2: A* 算法
    a_star_scheduler = AStarScheduler(network, path_finder)
    for job in workload_data:
        a_star_scheduler.schedule_job(job)
    a_star_total_bw = a_star_scheduler.get_total_peak_bandwidth()
    print(f"A*: {a_star_total_bw:.2f}") # Gbps
 
    a_star_file = os.path.join(result_dir, "a_star_result.json")
    with open(a_star_file, 'w') as f:
        json.dump(a_star_scheduler.format_results(), f, indent=2)

    # 策略 3: PDA 算法
    pda_scheduler = PDAScheduler(network, path_finder)
    for job in workload_data:
        pda_scheduler.schedule_job(job)
    pda_total_bw = pda_scheduler.get_total_peak_bandwidth()
    print(f"PDA: {pda_total_bw:.2f}") # Gbps

    pda_file = os.path.join(result_dir, "pda_result.json")
    with open(pda_file, 'w') as f:
        json.dump(pda_scheduler.format_results(), f, indent=2)

    # 策略 4：贪心算法
    greedy_scheduler = GreedyScheduler(network, path_finder)
    for job in workload_data:
        greedy_scheduler.schedule_job(job)
    greedy_total_bw = greedy_scheduler.get_total_peak_bandwidth()
    print(f"Greedy: {greedy_total_bw:.2f}") # Gbps

    greedy_file = os.path.join(result_dir, "greedy_result.json")
    with open(greedy_file, 'w') as f:
        json.dump(greedy_scheduler.format_results(), f, indent=2)

if __name__ == '__main__':
    
    topology_file = 'multi_job/link_list_infi.csv'
    
    for i in range(1, 51):
        workload_file = f'testcases/testcase{i}.json'
        if os.path.exists(workload_file):
            print(f"\ntestcase {i}")
            try:
                compare_schedulers(topology_file, workload_file)
            except Exception as e:
                print(f"Error processing testcase {i}: {str(e)}")
        else:
            print(f"Testcase {i} not found: {workload_file}")

    # # 测试 testcases/testcase1.json
    # workload_file = 'testcases/testcase1.json'
    # compare_schedulers(topology_file, workload_file)

