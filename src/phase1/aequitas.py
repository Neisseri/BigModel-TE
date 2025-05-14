import gurobipy as gp
from gurobipy import GRB
from dataclasses import dataclass
import numpy as np
from typing import Optional
import copy
import sys
import os
from job.job_info import JobInfo
import random
from job.workload import Workload

# 动态添加项目根目录到 sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from network.graph import Graph, Link
from network.path_finder import PathFinder
from params import SCHEDULE_INTERVAL
# 每隔 SCHEDULE_INTERVAL 进行一次流量调度，因此最多考虑 SCHEDULE_INTERVAL 个 epoch 的重叠周期即可

# 定义宏来简化变量类型
Tunnel = list[Link]

@dataclass
class Traffic:
    job_id: int
    cycle: int # (epoch)
    t_s: int # (epoch)
    t_e: int # (epoch)
    bw: float # (Gbps)
    
# 单个任务的调度
@dataclass
class JobSchedule:
    admit: int # 是否准入
    start_time: int  # 任务启动时间（epoch）
    tunnels: list[Tunnel] # 每个负载的隧道
    bw_alloc: list[float] # 每个负载在隧道上分配的带宽
    # TODO: 这里只考虑每个负载单条流的情况，bw_alloc 一定等于负载的 bw，后续输入多条隧道时再进行修改

# 三个优先级权重
PC_weight = 8
NC_weight = 4
BE_weight = 1

class Priority:
    PC = 0
    NC = 1
    BE = 2

class Aequitas():

    def __init__(self, network: Graph):

        self.network: Graph = network

        self.jobs: list[JobInfo] = [] # 任务列表

        # 任务优先级
        self.jobs_pri: dict[int, Priority] = {} # job_id -> Priority

        # 算路器，提供隧道
        self.path_finder = PathFinder(network)

        # 链路流量模式
        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 链路所有流量变化的时间点
        self.change_points: dict[int, set[int]] = {} # link_id -> set[int]
        # 任务调度
        self.job_schedules: dict[int, JobSchedule] = {} # job_id -> JobSchedule
        # 链路准入概率
        self.link_admit_prob: list[float] = [] 

    def add_traffic(self, link_id: int, traffic: Traffic) -> None:
        
        if link_id not in self.link_traffic:
            self.link_traffic[link_id] = []
            self.change_points[link_id] = set()
        
        # 添加流量
        self.link_traffic[link_id].append(traffic)

        # 重叠流量周期
        circle_list = []
        for traffic in self.link_traffic[link_id]:
            circle_list.append(traffic.cycle // 10 * 10)
        overlap_circle = np.lcm.reduce(circle_list) # 重叠流量周期
        # overlap_circle = SCHEDULE_INTERVAL

        # 添加新流量的变化时间点
        for circle_offset in range(0, overlap_circle, traffic.cycle):
            start = (traffic.t_s + circle_offset + self.job_schedules[traffic.job_id].start_time) % overlap_circle
            end = (traffic.t_e + circle_offset + self.job_schedules[traffic.job_id].start_time) % overlap_circle
                        
            self.change_points[link_id].add(start)
            self.change_points[link_id].add(end)
    
    def calculate_remaining_bw(self, tunnel: Tunnel, workload: Workload, cycle: int) -> float:
        remaining_bw = float('inf')
        for link in tunnel:
            link_alloc_bw = 0.0
            # 计算链路上已经分配的带宽
            for time in sorted(list(self.change_points[link.link_id])):
                if time % cycle >= workload.t_s and time % cycle < workload.t_e:
                    None
                else:
                    continue
                bw_now = 0.0
                for traffic in self.link_traffic[link.link_id]:
                    time_in_circle = time % traffic.cycle
                    if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                        bw_now += traffic.bw
                link_alloc_bw = max(link_alloc_bw, bw_now)
            remaining_bw = min(remaining_bw, link.capacity - link_alloc_bw)
        return remaining_bw
    
    def deploy(self, jobs: list[JobInfo]) -> list[int]:

        # a[j]={0, 1} 表示任务是否准入
        a: list[int] = []

        # 设置任务初始优先级
        cnt = 0
        for job in jobs:
            job_id = job.job_id
            cnt += 1
            if cnt <= (PC_weight / (PC_weight + NC_weight + BE_weight)) * len(jobs):
                self.jobs_pri[job_id] = Priority.PC
            elif cnt <= ((PC_weight + NC_weight) / (PC_weight + NC_weight + BE_weight)) * len(jobs):
                self.jobs_pri[job_id] = Priority.NC
            else:
                self.jobs_pri[job_id] = Priority.BE
            a.append(1)

            # 初始化所有任务调度结果
            self.job_schedules[job_id] = JobSchedule(
                admit = 1,
                start_time = 0,
                tunnels = [],
                bw_alloc = []
            )

        for link_id in range(self.network.link_num):
            self.link_admit_prob.append(1.0) # 初始准入概率为 100%

        job_cnt = -1
        for job in jobs:
            job_cnt += 1
            job_id = job.job_id
            rollback_workload_tag = -1
            for workload in job.workloads:
                rollback_workload_tag += 1
                tunnels: list[Tunnel] = self.path_finder.find_multi_path(workload.src, workload.dst)
                selected_tunnel: Tunnel = []
                max_admit_prob = 0.0
                for tunnel in tunnels:
                    # 计算隧道准入概率
                    tunnel_admit_prob = 1.0
                    for link in tunnel:
                        link_admit_prob = self.link_admit_prob[link.link_id]
                        if link_admit_prob < tunnel_admit_prob:
                            tunnel_admit_prob = link_admit_prob
                    if tunnel_admit_prob > max_admit_prob:
                        max_admit_prob = tunnel_admit_prob
                        selected_tunnel = tunnel
                self.job_schedules[job_id].tunnels.append(selected_tunnel)
                remaining_bw = calculate_remaining_bw(selected_tunnel, workload, job.cycle)
                if remaining_bw >= workload.bw:
                    self.job_schedules[job_id].bw_alloc.append(workload.bw)
                    for link in selected_tunnel:
                        self.link_admit_prob[link.link_id] *= 1.0 - (workload.bw / link.capacity)
                        # 更新链路流量模式
                        traffic = Traffic(
                            job_id = job_id,
                            cycle = job.cycle,
                            t_s = workload.t_s,
                            t_e = workload.t_e,
                            bw = workload.bw
                        )
                        self.add_traffic(link.link_id, traffic)
                else:
                    a[job_cnt] = 0
                    self.job_schedules[job_id].admit = 0
                    break
            # 如果准入失败则回滚
            if a[job_cnt] == 0:
                for workload_id in range(rollback_workload_tag + 1):
                    workload = job.workloads[workload_id]
                    selected_tunnel = self.job_schedules[job_id].tunnels[workload_id]
                    for link in selected_tunnel:
                        self.link_admit_prob[link.link_id] /= 1.0 - (workload.bw / link.capacity)
                        self.link_traffic[link.link_id].pop()
        
        return a
