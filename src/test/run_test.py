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
from phase1.aequitas import Aequitas
from phase1.seawall import Seawall
from phase2.traffic_schedule import TrafficScheduler
from phase2.greedy import Greedy
from phase2.ncflow import NCFlow
from phase2.igr import IGR
from job.job_info import JobInfo, EPOCH
from job.workload import Workload
from workload_fluctuate import random_fluctuate
from params import SCHEDULE_INTERVAL
from baseline.admission_control_bl import FCFS

measure_runtime: list[int] = []

# Phase 1
admit_rate: list[float] = []
adjust_rate: list[int] = []

# Phase 2
total_flow: list[float] = []
traffic_rate: list[float] = []
job_start_time: list[float] = []

# 全局变量：准入结果文件路径
ADMISSION_RESULT_FILE = 'result/admission_result.txt'
# 全局变量：流量调度结果文件路径
TRAFFIC_SCHEDULE_RESULT_FILE = 'result/traffic_schedule_result.txt'

# 网络拓扑
network: Graph = None

def run_admission_control(jobs_file: str, scenario: str, strategy: str) -> None:
    
    # 加载任务
    with open(jobs_file, 'r') as f:
        jobs_data = json.load(f)
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

    print(f"Admission Control {jobs_file}: {scenario} {strategy}")
    start_time = time.time()

    # 输出：a_j = {0, 1}，任务 j 是否准入
    a = [0] * len(jobs_data)

    if scenario == "FCFS":
        None
    elif scenario == "SJF":
        jobs = sorted(jobs, key=lambda x: sum(w.bw for w in x.workloads))

    # 准入策略
    if strategy == "Ours":
        admission_controller = AdmissionController(network)
        adjust_time = 0 # 局部调整次数
        
        for job_id, job in enumerate(jobs):
            # Step 1：直接部署
            a[job_id] = admission_controller.direct_deploy(job)
            # Step 2: 局部调整
            if a[job_id] == 0:
                a[job_id] = admission_controller.local_adjust(job)
                adjust_time += 1
            print(f"{job_id}/{len(jobs)} admit = {a[job_id]}")
        
        adjust_rate.append(adjust_time/len(jobs))

        os.makedirs(os.path.dirname(ADMISSION_RESULT_FILE), exist_ok=True)
        with open(ADMISSION_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    if link.link_id not in admission_controller.link_peak_bw:
                        admission_controller.link_peak_bw[link.link_id] = 0.0
                    f.write(f"{admission_controller.link_peak_bw[link.link_id] / link.capacity}\n")

    elif strategy == "BATE":
        admission_controller = AdmissionController(network)
        for job_id, job in enumerate(jobs):
            print(f"Processing: {job_id}/{len(jobs)}")
            a[job_id] = admission_controller.direct_deploy(job)

        os.makedirs(os.path.dirname(ADMISSION_RESULT_FILE), exist_ok=True)
        with open(ADMISSION_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    if link.link_id not in admission_controller.link_peak_bw:
                        admission_controller.link_peak_bw[link.link_id] = 0.0
                    f.write(f"{admission_controller.link_peak_bw[link.link_id] / link.capacity}\n")

    elif strategy == "Aequitas":
        admission_controller = Aequitas(network)
        a = admission_controller.deploy(jobs)

        os.makedirs(os.path.dirname(ADMISSION_RESULT_FILE), exist_ok=True)
        with open(ADMISSION_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    if link.link_id not in admission_controller.link_peak_bw:
                        admission_controller.link_peak_bw[link.link_id] = 0.0
                    f.write(f"{admission_controller.link_peak_bw[link.link_id] / link.capacity}\n")
    
    elif strategy == "Seawall":
        admission_controller = Seawall(network)
        a = admission_controller.deploy(jobs)
        
        os.makedirs(os.path.dirname(ADMISSION_RESULT_FILE), exist_ok=True)
        with open(ADMISSION_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    if link.link_id not in admission_controller.link_peak_bw:
                        admission_controller.link_peak_bw[link.link_id] = 0.0
                    f.write(f"{admission_controller.link_peak_bw[link.link_id] / link.capacity}\n")

    else:
        raise ValueError(f"Unknown strategy: {strategy}")
    
    end_time = time.time()
    measure_runtime.append(int((end_time - start_time) * 1000 / len(jobs)))

    print("Admitted jobs / Total jobs = ", sum(a), "/", len(jobs), 
        " = ", sum(a) / len(jobs))
    
    # 保留小数点后 4 位
    admit_rate.append(sum(a) / len(jobs))

    # 生成每个测例的调度结果目录
    # testcase_name = os.path.splitext(os.path.basename(jobs_file))[0]
    # result_dir = os.path.join("result", testcase_name)
    # os.makedirs(result_dir, exist_ok=True)
    # 保存调度结果
    # with open(result_dir + '/phase1_job_schedules.txt', 'w') as f:
    #     f.write(json.dumps(admission_controller.job_schedules, default=lambda x: x.__dict__, indent=4))

    # 保存所有测例的准入结果到文件
    # os.makedirs(os.path.dirname(ADMISSION_RESULT_FILE), exist_ok=True)
    # with open(ADMISSION_RESULT_FILE, 'a') as f:
    #     f.write(f"{os.path.basename(jobs_file)}: {sum(a)} / {len(jobs_data)} = {sum(a) / len(jobs_data):.2f}\n")

def run_traffic_schedule(jobs_file: str, strategy: str) -> None:

    # 加载任务
    with open(jobs_file, 'r') as f:
        jobs_data = json.load(f)
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
    
    print(f"Traffic Scheduling {jobs_file}: {strategy}")

    # 从文件中读取 Phase 1 调度结果
    testcase_name = os.path.splitext(os.path.basename(jobs_file))[0]
    result_dir = os.path.join("result", testcase_name)
    schedules: dict[int, JobSchedule] = {}
    with open(result_dir + '/phase1_job_schedules.txt', 'r') as f:
        schedules_data = json.load(f)
    # 将调度结果转换为 JobSchedule 对象
    for job_id, schedule in schedules_data.items():
        job_id = int(job_id)
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
        schedules[job_id] = JobSchedule(
            admit = int(schedule['admit']),
            start_time = int(schedule['start_time']),
            tunnels = tunnels,
            bw_alloc = list(map(float, schedule['bw_alloc']))
        )
        # NOTE：这里jobs[job_id].cycle可能index报错，因为jobs是list，之后可以改成dict，key是job_id
        # if schedules[job_id].admit == 1:
        #     job_start_time.append(schedules[job_id].start_time / jobs[job_id].cycle)
        
    new_jobs: dict[int, JobInfo] = {}
    for job in jobs:
        if job.job_id not in schedules:
            continue
        if schedules[job.job_id].admit == 1:
            new_jobs[job.job_id] = job
    
    start_time = time.time()

    if strategy == "Ours":

        traffic_scheduler = TrafficScheduler(network, new_jobs, schedules)
        flow, total_workload_bw = traffic_scheduler.update_schedule()
        print("Allocated Total Flow: ", flow)
        total_flow.append(flow)
        traffic_rate.append(flow / total_workload_bw)    

        with open(TRAFFIC_SCHEDULE_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    peak_bw = traffic_scheduler.calculate_peak_bw(link.link_id)
                    f.write(f"{peak_bw / link.capacity}\n") 
    
    elif strategy == "Greedy":
    
        traffic_scheduler = Greedy(network, new_jobs, schedules)
        flow, total_workload_bw = traffic_scheduler.greedy_alloc()
        print("Allocated Total Flow: ", flow)
        total_flow.append(flow)
        traffic_rate.append(flow / total_workload_bw)

        with open(TRAFFIC_SCHEDULE_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    traffic_scheduler.calculate_peak_bw(link.link_id)
                    peak_bw = traffic_scheduler.link_peak_bw[link.link_id]
                    f.write(f"{peak_bw / link.capacity}\n") 

    elif strategy == "NCFlow":

        traffic_scheduler = NCFlow(network, new_jobs, schedules)
        flow, total_workload_bw = traffic_scheduler.schedule()
        print("Allocated Total Flow: ", flow)
        total_flow.append(flow)
        traffic_rate.append(flow / total_workload_bw)

        with open(TRAFFIC_SCHEDULE_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    traffic_scheduler.calculate_peak_bw(link.link_id)
                    available_bw = traffic_scheduler.link_peak_bw[link.link_id]
                    f.write(f"{(link.capacity - available_bw) / link.capacity}\n") 
    
    elif strategy == "IGR":

        traffic_scheduler = IGR(network, new_jobs, schedules)
        flow, total_workload_bw = traffic_scheduler.schedule()
        print("Allocated Total Flow: ", flow)
        total_flow.append(flow)
        traffic_rate.append(flow / total_workload_bw)

        with open(TRAFFIC_SCHEDULE_RESULT_FILE, 'a') as f:
            for node in network.nodes:
                for link in network.edges[node]:
                    traffic_scheduler.calculate_peak_bw(link.link_id)
                    peak_bw = traffic_scheduler.link_peak_bw[link.link_id]
                    f.write(f"{peak_bw / link.capacity}\n") 

    end_time = time.time()
    measure_runtime.append(int((end_time - start_time) * 1000 / len(new_jobs)))

    # 保存流量总和到文件
    # with open(TRAFFIC_SCHEDULE_RESULT_FILE, 'a') as f:
    #     f.write(f"{os.path.basename(jobs_file)} {total_flow} {total_workload_bw}\n")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Run test with different strategies.")
    parser.add_argument("--phase", type=int, default=1,
                        choices=[1, 2], 
                        help="Phase to Run (1 or 2, default: 1)")
    parser.add_argument("--scenario", type=str, default="FCFS", 
                    choices=["FCFS", "SJF"], 
                    help="Admission Control Scenario (default: FCFS)")
    parser.add_argument("--strategy1", type=str, default="Ours", 
                        choices=["Ours", "BATE", "Aequitas", "Seawall"], 
                        help="Admission Control Strategy (default: Ours)")
    parser.add_argument("--strategy2", type=str, default="Ours",
                        choices=["Ours", "Greedy", "NCFlow", "IGR"], 
                        help="Traffic Scheduling Strategy (default: Ours)")
    args = parser.parse_args()

    # 加载拓扑
    topology_file = 'data/topology/link_list_tmp.csv'
    topology_df = pd.read_csv(topology_file)
    network: Graph = Graph.from_dataframe(topology_df)

    for i in range(1, 51):
        jobs_file = f'data/jobs/testcase{i}.json'
        if os.path.exists(jobs_file):
            print(f"\ntestcase {i}")
            try:
                if args.phase == 1:
                    run_admission_control(jobs_file, args.scenario, args.strategy1)
                elif args.phase == 2:
                    run_traffic_schedule(jobs_file, args.strategy2)
            except Exception as e:
                print(f"Error processing testcase {i}: {str(e)}")
        else:
            print(f"Testcase {i} not found: {jobs_file}")

    # 只跑第一个测例
    job_file = 'data/jobs/testcase49.json'
    # run_admission_control(job_file, args.scenario, args.strategy1)
    # run_traffic_schedule(job_file, args.strategy2)

    if args.phase == 1:
        print(f"Phase 1: {args.scenario} {args.strategy1}")
        # 准入率
        print(f"Admit Rate: {[round(rate, 4) for rate in admit_rate]}")
        print("Average admit rate:", sum(admit_rate) / len(admit_rate))
        # 平均单个任务准入时间
        print("Runtime:", measure_runtime)
        print("Average runtime:", sum(measure_runtime) / len(measure_runtime))
        if args.strategy1 == "Ours":
            print("Average adjust rate:", sum(adjust_rate) / len(adjust_rate))
    elif args.phase == 2:
        print(job_start_time)
        print(f"Phase 2: {args.strategy2}")
        print("Runtime:", measure_runtime)
        print("Average Runtime:", sum(measure_runtime) / len(measure_runtime))
        print("Total Flow:", total_flow)
        print("Average Total Flow:", sum(total_flow) / len(total_flow))
        print("Traffic Rate:", traffic_rate)
        print("Average Traffic Rate:", sum(traffic_rate) / len(traffic_rate))

