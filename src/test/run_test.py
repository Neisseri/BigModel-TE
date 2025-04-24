import json
import os
import pandas as pd
import time
import argparse  # 添加 argparse 模块

# 将父目录（即 src）添加到包导入搜索路径中
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.graph import Graph
from phase1.admission_control import AdmissionController
from phase2.traffic_schedule import TrafficScheduler
from job.job_info import JobInfo, EPOCH
from job.workload import Workload
from workload_fluctuate import random_fluctuate
from params import SCHEDULE_INTERVAL
from baseline.admission_control_bl import FCFS

# 是否启用 Phase 2 流量调度
TRAFFIC_SCHEDULE_SWITCH = False

# 全局变量：准入结果文件路径
ADMISSION_RESULT_FILE = 'result/admission_result.txt'

def run_admission_control(topology_file: str, jobs_file: str, strategy: str) -> None:
    print(f"Running admission control for {jobs_file} with strategy {strategy}...")

    # 加载拓扑
    topology_df = pd.read_csv(topology_file)
    # 加载任务
    with open(jobs_file, 'r') as f:
        jobs_data = json.load(f)
    # 生成测例结果目录
    testcase_name = os.path.splitext(os.path.basename(jobs_file))[0]
    result_dir = os.path.join("result", testcase_name)
    os.makedirs(result_dir, exist_ok=True)

    # 输入：网络 + 任务
    # 网络拓扑
    network: Graph = Graph.from_dataframe(topology_df)
    # 任务信息
    jobs: list[JobInfo] = []
    for job in jobs_data:
        job_id = job['job_id']
        cycle = (job['cycle(ms)'] + EPOCH - 1) // EPOCH # 向上取整
        workloads = []
        for workload in job['workloads']:
            src = workload['src_rank']
            dst = workload['dst_rank']
            t_s = workload['start_timestamp(ms)'] // EPOCH # 向下取整
            t_e = (workload['end_timestamp(ms)'] + EPOCH - 1) // EPOCH # 向上取整
            bw = workload['bandwidth(Gbps)']
            workload = Workload(src, dst, t_s, t_e, bw)
            workloads.append(workload)
        job_info = JobInfo(job_id, cycle, workloads)
        jobs.append(job_info)

    # Phase 1：准入控制

    # 输出：
    # a_j = {0, 1}，表示任务 j 是否准入
    a = [0] * len(jobs_data)

    # 根据策略选择准入控制方法
    if strategy == "TE":

        # 跨域大模型流量工程方案
        admission_controller = AdmissionController(network)
        for job_id, job in enumerate(jobs):
            print(f"Processing job {job_id}...")

            # Step 1：直接部署
            a[job_id] = admission_controller.direct_deploy(job)
            # Step 2: 局部调整
            if a[job_id] == 0:
                a[job_id] = admission_controller.local_adjust(job)

    elif strategy == "FCFS":
        # FCFS_controller = FCFS(network)
        # for job_id, job in enumerate(jobs):
        #     print(f"Processing job {job_id}...")
        #     job_demand = FCFS_controller.get_demand(job)

        #     a[job_id] = FCFS_controller.direct_deploy(job_demand)
        #     # if a[job_id] == 0:
        #     #     a[job_id] = FCFS_controller.reschedule(job_demand)
        admission_controller = AdmissionController(network)
        for job_id, job in enumerate(jobs):
            print(f"Processing job {job_id}...")

            # Step 1：直接部署
            a[job_id] = admission_controller.direct_deploy(job)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    print("Admitted jobs / Total jobs = ", sum(a), "/", len(jobs_data), 
          " = ", sum(a) / len(jobs_data))

    # 保存准入结果到文件
    with open(ADMISSION_RESULT_FILE, 'a') as f:
        f.write(f"{os.path.basename(jobs_file)}: {sum(a)} / {len(jobs_data)} = {sum(a) / len(jobs_data):.2f}\n")

    # Phase 2：流量调度
    if TRAFFIC_SCHEDULE_SWITCH:
        
        traffic_scheduler = TrafficScheduler(network, jobs, admission_controller.job_schedules)
        while (True):
            # 程序等待 （EPOCH * SCHEDULE_INTERVAL）ms 的时间
            wait_time = EPOCH * SCHEDULE_INTERVAL / 1000  # 转换为秒
            time.sleep(wait_time)
            new_jobs = random_fluctuate(jobs)
            new_schedules = traffic_scheduler.update_schedule(new_jobs)

if __name__ == '__main__':
    # 添加命令行参数解析
    parser = argparse.ArgumentParser(description="Run admission control with different strategies.")
    parser.add_argument("--strategy", type=str, default="TE", 
                        choices=["TE", "FCFS"], 
                        help="Admission control strategy to use (default: TE)")
    args = parser.parse_args()

    # 创建一个空的新文件
    os.makedirs(os.path.dirname(ADMISSION_RESULT_FILE), exist_ok=True)
    with open(ADMISSION_RESULT_FILE, 'w') as f:
        f.write("Admission Results:\n")

    topology_file = 'data/topology/link_list_tmp.csv'

    for i in range(1, 51):
        jobs_file = f'data/jobs/testcase{i}.json'
        if os.path.exists(jobs_file):
            print(f"\ntestcase {i}")
            try:
                run_admission_control(topology_file, jobs_file, args.strategy)
            except Exception as e:
                print(f"Error processing testcase {i}: {str(e)}")
        else:
            print(f"Testcase {i} not found: {jobs_file}")

    # 只跑第一个测例
    # job_file = 'data/jobs/testcase1.json'
    # run_admission_control(topology_file, job_file, args.strategy)

