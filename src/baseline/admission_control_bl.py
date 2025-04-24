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

# 基于 BATE 中的准入控制策略实现

@dataclass
class Demand:
    src: int
    dst: int
    bw: float

class FCFS():

    def __init__(self, network: Graph):

        self.network: Graph = network
        self.path_finder: PathFinder = PathFinder(network)
        self.link_alloc: dict[int, float] = {}

    def get_demand(self, job: JobInfo) -> list[Demand]:
        # 获取任务的需求，相同 (src, dst) 的带宽取最大值
        demand_dict = {}
        for workload in job.workloads:
            src, dst, bw = workload.src, workload.dst, workload.bw
            if (src, dst) not in demand_dict:
                demand_dict[(src, dst)] = bw
            else:
                demand_dict[(src, dst)] = max(demand_dict[(src, dst)], bw)
        return [Demand(src, dst, bw) for (src, dst), bw in demand_dict.items()]

    def direct_deploy(self, demands: list[Demand]) -> int:
        
        job_link_alloc: dict[int, float] = {}
        # 遍历需求列表
        for demand in demands:
            src, dst, bw = demand.src, demand.dst, demand.bw

            path = self.path_finder.find_path(src, dst)
            if not path:
                return 0  # 无法找到路径，直接拒绝

            # 检查路径上的链路是否有足够的剩余容量
            for link in path:
                if link.link_id not in self.link_alloc:
                    self.link_alloc[link.link_id] = 0
                if (link.capacity - self.link_alloc[link.link_id]) < bw:
                    return 0  # 链路容量不足，直接拒绝
                
                if link.link_id not in job_link_alloc:
                    job_link_alloc[link.link_id] = 0
                job_link_alloc[link.link_id] = max(job_link_alloc[link.link_id], bw)

        for link_id, bw in job_link_alloc.items():
            if link_id not in self.link_alloc:
                self.link_alloc[link_id] = 0
            self.link_alloc[link_id] += bw

        return 1  # 成功部署需求

    def reschedule(self, demands: list[Demand]) -> int:

        # TODO: 怎么用 Demand 表示还需要再明确一下

        # 重编排已接受的需求以适应新需求

        # 备份当前网络状态
        original_edges = copy.deepcopy(self.network.edges)

        # 遍历需求列表
        for demand in demands:
            src, dst, bw = demand.src, demand.dst, demand.bw

            # 贪心算法：优先选择剩余容量和可用性乘积较小的隧道
            path = self.path_finder.find_path(src, dst)
            if not path:
                self.network.edges = original_edges  # 恢复网络状态
                return 0

            # 检查路径上的链路是否有足够的剩余容量
            for link in path:
                if link.capacity < bw:
                    self.network.edges = original_edges  # 恢复网络状态
                    return 0  # 链路容量不足，拒绝

            for link in path:
                link.capacity -= bw

        return 1  # 成功重编排需求