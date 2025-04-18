import gurobipy as gp
from gurobipy import GRB
from dataclasses import dataclass
from .demand import Demand
from .graph import Graph, Link
from .path_finder import PathFinder
import numpy as np
from typing import Optional
import copy
from .scheduler_base import SchedulerBase, ScheduleResult

@dataclass
class LinkTrafficPattern:
    job_id: int
    cycle: int # (ms)
    start_time: int # (ms)
    end_time: int # (ms)
    bandwidth: float # (Gbps)

    def copy(self):
        return copy.deepcopy(self)

class GreedyScheduler(SchedulerBase):  # 修改：继承 SchedulerBase

    def __init__(self, graph: Graph, path_finder: PathFinder):
        super().__init__(graph, path_finder)  # 添加：调用父类初始化
        self.job_start_time: dict[int, int] = {}  # 添加：记录每个任务的最优启动时间
        self.total_traffic_demand = 0.0  # 添加：记录所有流量需求的总和

    def find_best_start_time(self, 
                             job_id: int, 
                             job_cycle_len: int, 
                             job_demands: list[Demand],
                             job_bw_alloc: dict[int, list[tuple[list[Link], float]]]
                             ) -> tuple[Optional[float], float]:
        # find all links that this job will pass
        passed_link_set: set[tuple[int, int]] = set()
        for demand_idx, demand_bw_alloc in job_bw_alloc.items():
            for path, _ in demand_bw_alloc:
                for link in path:
                    link_pair = (link.src_rank, link.dst_rank)
                    if link_pair in passed_link_set:
                        continue
                    passed_link_set.add(link_pair)
                    # add traffic pattern for this link and save original
                    if link_pair not in self.link_traffic_patterns:
                        self.link_traffic_patterns[link_pair] = []
                    pattern = LinkTrafficPattern(
                        job_id = job_id,
                        cycle = job_cycle_len,
                        start_time = job_demands[demand_idx].start_time,
                        end_time = job_demands[demand_idx].end_time, 
                        bandwidth = job_demands[demand_idx].bandwidth
                    )
                    self.link_traffic_patterns[link_pair].append(pattern)

        # 将当前任务经过的链路添加到全局链路集合中
        self.all_links.update(passed_link_set)

        min_peak_bw_sum = float('inf')
        best_start_time = 0
        
        # 在所有可能的时间点中寻找最优解
        for start_time in range(0, job_cycle_len, self.time_precision):
            # 临时记录在当前启动时间下每条链路的带宽
            temp_link_bandwidth: dict[tuple[int, int], float] = {}
            temp_peak_bw_sum = 0.0
            
            for link_pair in self.all_links:
                peak_bw = 0.0
                circle_list = []
                
                # 计算统一周期
                for traffic_pattern in self.link_traffic_patterns[link_pair]:
                    rounded_cycle = ((traffic_pattern.cycle + self.circle_precision // 2) // 
                                   self.circle_precision) * self.circle_precision
                    circle_list.append(rounded_cycle)
                
                if circle_list:
                    unified_circle = min(np.lcm.reduce(circle_list), self.max_overlap_circle)
                    
                    # 收集所有流量变化点
                    change_points = set()
                    for pattern in self.link_traffic_patterns[link_pair]:
                        for t in range(0, unified_circle, pattern.cycle):
                            if pattern.job_id == job_id:
                                start = (pattern.start_time + t + start_time) % unified_circle
                                end = (pattern.end_time + t + start_time) % unified_circle
                            else:
                                start = (pattern.start_time + t + self.job_start_time[pattern.job_id]) % unified_circle
                                end = (pattern.end_time + t + self.job_start_time[pattern.job_id]) % unified_circle
                            
                            change_points.add(start)
                            change_points.add(end)
                    
                    # 在每个流量变化点计算带宽
                    for time in sorted(list(change_points)):
                        bw_this_time = 0.0
                        for pattern in self.link_traffic_patterns[link_pair]:
                            if pattern.job_id == job_id:
                                time_in_job = (time + pattern.cycle - start_time) % pattern.cycle
                            else:
                                time_in_job = (time + pattern.cycle - self.job_start_time[pattern.job_id]) % pattern.cycle
                            if time_in_job >= pattern.start_time and time_in_job < pattern.end_time:
                                bw_this_time += pattern.bandwidth
                        peak_bw = max(peak_bw, bw_this_time)
                
                temp_link_bandwidth[link_pair] = peak_bw
                temp_peak_bw_sum += peak_bw
            
            # 如果当前的带宽和小于最小值，则更新最优解
            if temp_peak_bw_sum < min_peak_bw_sum:
                min_peak_bw_sum = temp_peak_bw_sum
                best_start_time = start_time
                # 更新每条链路的峰值带宽
                for link_pair, peak_bw in temp_link_bandwidth.items():
                    self.link_peak_bandwidth[link_pair] = peak_bw

        return best_start_time, min_peak_bw_sum

    def schedule_job(self, job: dict) -> ScheduleResult:
        job_id = job['job_id']
        job_bw_alloc: dict[int, list[tuple[list[Link], float]]] = {}
        job_total_alloc = 0.0

        demands = []
        
        for demand_idx, demand_data in enumerate(job['demands']):
            demand = Demand(
                job_id=job_id,
                src_rank=demand_data['src_rank'],
                dst_rank=demand_data['dst_rank'],
                start_time=demand_data['start_timestamp(ms)'],
                end_time=demand_data['end_timestamp(ms)'],
                bandwidth=demand_data['bandwidth(Gbps)']
            )
            # 累加总带宽需求
            self.total_traffic_demand += demand.bandwidth

            demands.append(demand)
            
            demand_bw_alloc = self.path_finder.allocate_demand_bandwidth(demand)
            if not demand_bw_alloc: # []: failed
                # rollback all previous allocations of this job
                # self._rollback_allocation(job_bw_alloc) 
                # TODO:
                return ScheduleResult(
                    job_id, False, 0, {}
                )
            
            job_bw_alloc[demand_idx] = demand_bw_alloc
            job_total_alloc += sum(bw for _, bw in demand_bw_alloc)

        self.job_start_time[job_id] = 0
        start_time, peak_bw = self.find_best_start_time(job['job_id'], job['cycle(ms)'], demands, job_bw_alloc)
        self.job_start_time[job_id] = start_time

        return ScheduleResult(
            job_id, True, start_time, job_bw_alloc
        )