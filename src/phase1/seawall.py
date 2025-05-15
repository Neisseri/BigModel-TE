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

class Seawall():

    def __init__(self, network: Graph):

        self.network: Graph = network

        self.jobs: list[JobInfo] = [] # 任务列表

        # 任务带宽配额
        self.jobs_quota: dict[int, int] = {} # job_id -> quota

        # 算路器，提供隧道
        self.path_finder = PathFinder(network)

        # 链路流量模式
        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 任务调度
        self.job_schedules: dict[int, JobSchedule] = {} # job_id -> JobSchedule

        self.link_peak_bw: dict[int, float] = {} # 链路上已经分配的带宽
        self.link_peak_bw_to_update: dict[int, bool] = {} # 需要更新的链路

    def add_traffic(self, link_id: int, traffic: Traffic) -> None:
        
        if link_id not in self.link_traffic:
            self.link_traffic[link_id] = []
        
        # 添加流量
        self.link_traffic[link_id].append(traffic)

        self.link_peak_bw_to_update[link_id] = True

    def calculate_remaining_bw(self, tunnel: Tunnel, workload: Workload, cycle: int) -> float:

        remaining_bw = float('inf')
        for link in tunnel:

            change_points: list[int] = []

            if link.link_id not in self.link_traffic:
                self.link_traffic[link.link_id] = []
                continue

            if self.link_traffic[link.link_id] == []:
                continue

            if link.link_id not in self.link_peak_bw:
                self.link_peak_bw[link.link_id] = 0.0
                self.link_peak_bw_to_update[link.link_id] = True

            link_alloc_bw = 0.0

            # 计算瓶颈带宽时注释掉
            # if self.link_peak_bw_to_update[link.link_id] == False:
            #     link_alloc_bw = self.link_peak_bw[link.link_id]
            #     remaining_bw = min(remaining_bw, link.capacity - link_alloc_bw)
            #     continue

            # 重叠流量周期
            circle_list: list[int] = []
            for traffic in self.link_traffic[link.link_id]:
                circle_list.append(traffic.cycle // 25 * 25)
            overlap_circle = np.lcm.reduce(circle_list) # 重叠流量周期
            # overlap_circle = SCHEDULE_INTERVAL

            for traffic in self.link_traffic[link.link_id]:
                # 添加新流量的变化时间点
                for circle_offset in range(0, overlap_circle, traffic.cycle):
                    start = (traffic.t_s + circle_offset + self.job_schedules[traffic.job_id].start_time) % overlap_circle
                    end = (traffic.t_e + circle_offset + self.job_schedules[traffic.job_id].start_time) % overlap_circle
                                
                    change_points.append(start)
                    change_points.append(end)

            # 计算链路上已经分配的带宽
            for time in sorted(list(change_points)):
                # 注释掉就是计算峰值带宽
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

            self.link_peak_bw[link.link_id] = link_alloc_bw
            self.link_peak_bw_to_update[link.link_id] = False

            remaining_bw = min(remaining_bw, link.capacity - link_alloc_bw)
        return remaining_bw
    
    def deploy(self, jobs: list[JobInfo]) -> list[int]:

        # a[j]={0, 1} 表示任务是否准入
        a: list[int] = []

        # 设置任务配额
        for job in jobs:
            job_id = job.job_id
            self.jobs_quota[job_id] = int(sum([workload.bw for workload in job.workloads]))
            a.append(1)

            # 初始化所有任务调度结果
            self.job_schedules[job_id] = JobSchedule(
                admit = 1,
                start_time = 0,
                tunnels = [],
                bw_alloc = []
            )

        job_cnt = -1
        for job in jobs:
            job_cnt += 1
            job_id = job.job_id
            rollback_workload_tag = -1
            for workload in job.workloads:
                rollback_workload_tag += 1
                tunnels: list[Tunnel] = self.path_finder.find_multi_path(workload.src, workload.dst)
                selected_tunnel: Tunnel = []
                max_quota_bw = 0.0
                for tunnel in tunnels:
                    # 计算隧道配额带宽
                    tunnel_quota_bw = 0
                    for link in tunnel:
                        if link.link_id not in self.link_traffic:
                            self.link_traffic[link.link_id] = []
                            tunnel_quota_bw = min(tunnel_quota_bw, link.capacity)
                            continue
                        link_quota_sum = 0
                        for traffic in self.link_traffic[link.link_id]:
                            link_quota_sum += self.jobs_quota[traffic.job_id]
                        tunnel_quota_bw += link.capacity * self.jobs_quota[job_id] / (link_quota_sum + self.jobs_quota[job_id])
                        
                    if tunnel_quota_bw > max_quota_bw:
                        max_quota_bw = tunnel_quota_bw
                        selected_tunnel = tunnel
                self.job_schedules[job_id].tunnels.append(selected_tunnel)
                remaining_bw = self.calculate_remaining_bw(selected_tunnel, workload, job.cycle)
                if remaining_bw >= workload.bw:
                    self.job_schedules[job_id].bw_alloc.append(workload.bw)
                    for link in selected_tunnel:
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
                        if self.link_traffic[link.link_id] == []:
                            continue
                        if self.link_traffic[link.link_id][-1].job_id == job_id:
                            self.link_traffic[link.link_id].pop()
            print(f"{job_cnt}/{len(jobs)} admit = {a[job_cnt]}")
        
        return a
