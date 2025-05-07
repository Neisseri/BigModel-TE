import json
import os
import pandas as pd
import time
import argparse  # 添加 argparse 模块

# 将父目录（即 src）添加到包导入搜索路径中
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.graph import Graph, Link
from phase1.admission_control import AdmissionController, JobSchedule
from phase2.traffic_schedule import TrafficScheduler
from job.job_info import JobInfo, EPOCH
from job.workload import Workload
from workload_fluctuate import random_fluctuate
from params import SCHEDULE_INTERVAL
from baseline.admission_control_bl import FCFS

# 全局变量：准入结果文件路径
ADMISSION_RESULT_FILE = 'result/admission_result.txt'
# 全局变量：流量调度结果文件路径
TRAFFIC_SCHEDULE_RESULT_FILE = 'result/traffic_schedule_result.txt'

def run_admission_control(topology_file: str, jobs_file: str, phase: int, strategy: str) -> None:
    
    # 加载拓扑
    topology_df = pd.read_csv(topology_file)
    # 加载任务
    with open(jobs_file, 'r') as f:
        jobs_data = json.load(f)
    testcase_name = os.path.splitext(os.path.basename(jobs_file))[0]
    result_dir = os.path.join("result", testcase_name)
    # 生成测例结果目录
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
    if phase == 1:
        print(f"Running admission control for {jobs_file} with strategy {strategy}...")

        # 输出：
        # a_j = {0, 1}，表示任务 j 是否准入
        a = [0] * len(jobs_data)

        # 根据策略选择准入控制方法
        if strategy == "TE":

            # 启用抢占式（带宽小的任务优先）
            jobs = sorted(jobs, key=lambda x: sum(w.bw for w in x.workloads))
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
        elif strategy == "SJF":
            # 总带宽需求小的任务优先
            priority_jobs = sorted(jobs, key=lambda x: sum(w.bw for w in x.workloads))
            admission_controller = AdmissionController(network)
            for job_id, job in enumerate(priority_jobs):
                print(f"Processing job {job_id}...")

                # Step 1：直接部署
                a[job_id] = admission_controller.direct_deploy(job)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

        print("Admitted jobs / Total jobs = ", sum(a), "/", len(jobs_data), 
            " = ", sum(a) / len(jobs_data))

        # 保存调度结果
        with open(result_dir + '/phase1_job_schedules.txt', 'w') as f:
            f.write(json.dumps(admission_controller.job_schedules, default=lambda x: x.__dict__, indent=4))

        # 保存准入结果到文件
        with open(ADMISSION_RESULT_FILE, 'a') as f:
            f.write(f"{os.path.basename(jobs_file)}: {sum(a)} / {len(jobs_data)} = {sum(a) / len(jobs_data):.2f}\n")

    elif phase == 2:
        # Phase 2：流量调度
        print("Running traffic scheduling...")
        # 从文件中读取调度结果
        old_schedules: dict[int, JobSchedule] = {}
        with open(result_dir + '/phase1_job_schedules.txt', 'r') as f:
            schedules_data = json.load(f)
        # 将调度结果转换为 JobSchedule 对象
        for job_id, schedule in schedules_data.items():
            job_id = int(job_id)
            # 将字典转换为 JobSchedule 对象
            tunnels: list[list[Link]] = []
            for tunnel_data in schedule['tunnels']:
                tunnel: list[Link] = []
                for link_data in tunnel_data:
                    link = Link(
                        link_id = link_data['link_id'],
                        src = link_data['src'],
                        dst = link_data['dst'],
                        capacity = link_data['capacity']
                    )
                    tunnel.append(link)
                tunnels.append(tunnel)
            old_schedules[job_id] = JobSchedule(
                admit = int(schedule['admit']),
                start_time = int(schedule['start_time']),
                tunnels = tunnels,
                bw_alloc = list(map(float, schedule['bw_alloc']))
            )

        traffic_scheduler = TrafficScheduler(network, jobs, old_schedules)
        while (True):
            # 程序等待 （EPOCH * SCHEDULE_INTERVAL）ms 的时间
            wait_time = EPOCH * SCHEDULE_INTERVAL / 1000  # 转换为秒
            time.sleep(wait_time)
            new_jobs = random_fluctuate(jobs)
            new_schedules, total_flow1 = traffic_scheduler.update_schedule(new_jobs)
            # 保存调度结果
            with open(result_dir + '/phase2_job_schedules.txt', 'w') as f:
                f.write(json.dumps(new_schedules, default=lambda x: x.__dict__, indent=4))
            
            total_flow2 = traffic_scheduler.greedy_alloc()

            # 保存流量总和到文件
            with open(TRAFFIC_SCHEDULE_RESULT_FILE, 'a') as f:
                f.write(f"{os.path.basename(jobs_file)}: TE = {total_flow1}, Greedy = {total_flow2}\n")
            
            # 只调度一次
            return

if __name__ == '__main__':
    # 添加命令行参数解析
    parser = argparse.ArgumentParser(description="Run test with different strategies.")
    parser.add_argument("--phase", type=int, default=1,
                        choices=[1, 2], 
                        help="Phase to run (1 or 2, default: 1)")
    parser.add_argument("--strategy", type=str, default="TE", 
                        choices=["TE", "FCFS", "SJF"], 
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
    # job_file = 'data/jobs/testcase6.json'
    # run_admission_control(topology_file, job_file, args.phase, args.strategy)

