import gurobipy as gp
from gurobipy import GRB
from dataclasses import dataclass
import numpy as np
from typing import Optional
import copy
import sys
import os
from job.job_info import JobInfo

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

class Bate():

    def __init__(self, network: Graph):

        self.network: Graph = network

        # 已发起部署请求的任务集合
        self.jobs: dict[int, JobInfo] = {}

        # 算路器，提供隧道
        self.path_finder = PathFinder(network)

        # 链路流量模式
        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 链路所有流量变化的时间点
        self.change_points: dict[int, set[int]] = {} # link_id -> set[int]
        # 链路峰值带宽
        self.link_peak_bw: dict[int, float] = {} # link_id -> peak_bandwidth
        # 链路峰值带宽所在时间点
        self.link_peak_bw_points: dict[int, int] = {} # link_id -> peak_bandwidth_time_point
        # 任务调度
        self.job_schedules: dict[int, JobSchedule] = {} # job_id -> JobSchedule
        
        # 参数设置
        self.strat_time_step = 10 # 枚举启动时间的步长

    def update_peak_bw(self, link_id: int) -> None:
        
        peak_bw = 0.0
        # 在每个流量变化时间点计算总带宽
        for time in sorted(list(self.change_points[link_id])):
            bw_now = 0.0
            for traffic in self.link_traffic[link_id]:
                job_id = traffic.job_id
                time_in_circle = (time + traffic.cycle - self.job_schedules[job_id].start_time) % traffic.cycle
                if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                    bw_now += traffic.bw
            if bw_now >= peak_bw:
                peak_bw = bw_now
                self.link_peak_bw_points[link_id] = time
        self.link_peak_bw[link_id] = peak_bw

    def add_traffic(self, link_id: int, traffic: Traffic) -> None:
        
        if link_id not in self.link_peak_bw:
            self.link_peak_bw[link_id] = 0.0
        if link_id not in self.link_peak_bw_points:
            self.link_peak_bw_points[link_id] = 0
        
        # 添加流量
        self.link_traffic[link_id].append(traffic)

        # 重叠流量周期
        # circle_list = []
        # for traffic in self.link_traffic[link_id]:
        #     circle_list.append(traffic.cycle)
        # overlap_circle = min(np.lcm.reduce(circle_list), SCHEDULE_INTERVAL) # 重叠流量周期
        # TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
        overlap_circle = SCHEDULE_INTERVAL

        # 添加新流量的变化时间点
        for circle_offset in range(0, overlap_circle, traffic.cycle):
            start = (traffic.t_s + circle_offset + self.job_schedules[traffic.job_id].start_time) % overlap_circle
            end = (traffic.t_e + circle_offset + self.job_schedules[traffic.job_id].start_time) % overlap_circle
                        
            self.change_points[link_id].add(start)
            self.change_points[link_id].add(end)

        # 更新链路峰值带宽
        self.update_peak_bw(link_id)
    
    def direct_deploy(self, job: JobInfo) -> int:
        
        job_id = job.job_id
        self.jobs[job_id] = job
        self.job_schedules[job_id] = JobSchedule(
            admit = 0,
            start_time = 0,
            tunnels = [],
            bw_alloc = []
        )

        # 基于贪心策略，尝试直接部署任务
        alloc_success = True
        for workload in job.workloads:
            tunnel: Tunnel = self.path_finder.find_path(workload.src, workload.dst)
            self.job_schedules[job_id].tunnels.append(tunnel)

            # TODO: 如果后续改为每个负载多条流，则这里需要遍历所有隧道依次分配带宽
            # TODO: 这里还需要考虑负载时间上重叠的情况（也就是说要进行临时分配）
            for link in tunnel:
                if self.link_peak_bw.get(link.link_id) is None:
                    self.link_peak_bw[link.link_id] = 0.0
                    self.link_peak_bw_points[link.link_id] = 0
                    self.link_traffic[link.link_id] = []
                    self.change_points[link.link_id] = set()

                if (link.capacity - self.link_peak_bw[link.link_id]) < workload.bw: # 链路剩余容量小于所需带宽
                    alloc_success = False
        if not alloc_success:
            # 直接部署失败
            return 0
                
        # 剩余容量充足，则任务准入
        self.job_schedules[job_id].admit = 1
        # 启动时间默认为 0
        self.job_schedules[job_id].start_time = 0
        # 分配带宽
        for workload_id, workload in enumerate(job.workloads):
            self.job_schedules[job_id].bw_alloc.append(workload.bw)
            
            # 更新链路流量模式
            traffic = Traffic(
                    job_id = job_id,
                    cycle = job.cycle,
                    t_s = workload.t_s,
                    t_e = workload.t_e,
                    bw = workload.bw
                )
            for link in self.job_schedules[job_id].tunnels[workload_id]:
                link_id = link.link_id
                if link_id not in self.link_traffic:
                    self.link_traffic[link_id] = []
                # 添加流量
                self.add_traffic(link_id, traffic)
        return 1

    def link_adjust(self, link_id: int, link_capacity: float) -> bool:
        # TODO: 由于当前每个负载分配一条流，所以只进行启动时间调度
        peak_bw_point = self.link_peak_bw_points[link_id]
        # 筛选 peak_bw_point 时刻所有活跃流量
        job_to_adjust: list[int, float] = []
        for traffic in self.link_traffic[link_id]:
            job_id = traffic.job_id
            time_in_circle = (peak_bw_point + traffic.cycle - self.job_schedules[job_id].start_time) % traffic.cycle
            if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                job_to_adjust.append((job_id, traffic.bw))
        job_to_adjust.sort(key=lambda x: x[1], reverse=True)

        # 优先调整带宽大的流量所属的任务启动时间
        for job in job_to_adjust:
            job_id = job[0]
            # 记录任务原有启动时间，用于后续回退
            original_start_time = self.job_schedules[job_id].start_time
            # 找到一个启动时间，使 self.link_peak_bw[link_id] <= link.capacity
            for start_time in range(0, self.jobs[job_id].cycle, self.strat_time_step):
                self.job_schedules[job_id].start_time = start_time
                self.update_peak_bw(link_id)
                if self.link_peak_bw[link_id] <= link_capacity:
                    # 该链路的局部调整成功（使总带宽没有超出链路容量）
                    # TODO: 还需要检查该任务经过的其他链路是否溢出
                    return True
            # 回退当前任务启动时间
            self.job_schedules[job_id].start_time = original_start_time
                
        return False
            

    def local_adjust(self, job: JobInfo) -> int:

        # 为了节约时间，仅调整限制次数
        max_call_time = 10

        job_id = job.job_id
        # 记录分配到了第几个负载，用于后续无法准入时回退
        rollback_count = 0
        
        tag = True
        for workload_id, workload in enumerate(job.workloads):

            tunnel: Tunnel = self.job_schedules[job_id].tunnels[workload_id]
            # 分配带宽
            traffic: Traffic = Traffic(
                    job_id = job_id,
                    cycle = job.cycle,
                    t_s = workload.t_s,
                    t_e = workload.t_e,
                    bw = workload.bw
                )
            for link in tunnel:
                rollback_count += 1
                self.add_traffic(link.link_id, traffic)
                if self.link_peak_bw[link.link_id] > link.capacity:
                    # 负载分配失败，尝试局部调整
                    if max_call_time > 0:
                        adjust_success = self.link_adjust(link.link_id, link.capacity)
                    else:
                        adjust_success = False

                    max_call_time -= 1
                    if adjust_success == False:
                        tag = False
                        break
            if tag == False:
                break
        
        if tag == False:
            # 回退已分配流量
            for workload_id, workload in enumerate(job.workloads):
                tunnel: Tunnel = self.job_schedules[job_id].tunnels[workload_id]
                for link in tunnel:

                    rollback_count -= 1
                    if rollback_count < 0:
                        return 0
                    
                    # 删除最后一个元素（即当前任务的流量）
                    self.link_traffic[link.link_id].pop()

        else:
            # 任务准入
            self.job_schedules[job_id].admit = 1
            # 启动时间默认为 0
            self.job_schedules[job_id].start_time = 0
            # 分配带宽
            for workload_id, workload in enumerate(job.workloads):
                self.job_schedules[job_id].bw_alloc.append(workload.bw)
            return 1
        