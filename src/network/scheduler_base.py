from dataclasses import dataclass
from abc import ABC, abstractmethod
from .graph import Graph, Link
from .path_finder import PathFinder, Demand
from typing import Optional, Dict, List, Tuple
import copy
import numpy as np

# 定义宏来简化变量类型
SDPair = tuple[int, int]
PathBwAlloc = list[list[SDPair], float]

@dataclass
class LinkTrafficPattern:
    job_id: int
    cycle: int # (ms)
    start_time: int # (ms)
    end_time: int # (ms)
    bandwidth: float # (Gbps)

    def copy(self):
        return copy.deepcopy(self)

# 单个任务的调度结果
@dataclass
class ScheduleResult:
    job_id: int
    success: bool
    start_time: float  # 任务启动时间
    paths_allocation: dict[int, PathBwAlloc] # demand_id -> [(path, bandwidth)]

class SchedulerBase(ABC):
    def __init__(self, graph: Graph, path_finder: PathFinder):
        self.graph = graph
        self.path_finder = path_finder
        self.link_traffic_patterns: dict[SDPair, list[LinkTrafficPattern]] = {} # 链路上经过的流量模式 link -> list[LinkTrafficPattern]
        self.link_peak_bandwidth: Dict[SDPair, float] = {} # 某一条链路此时的峰值带宽
        self.all_links: set[SDPair] = set() # 所有出现过流量的链路
        self.results: List[ScheduleResult] = []
        
        # 参数设置
        self.time_precision = 100 # 枚举启动时间的步长
        self.circle_precision = 100 # 计算整体流量模式周期的舍入精度
        self.max_overlap_circle = 10000 # 整体流量模式周期上限

    @abstractmethod
    def schedule_job(self, job: Dict) -> ScheduleResult:
        pass

    def get_total_peak_bandwidth(self) -> float:
        """获取所有链路的峰值带宽和"""
        return sum(self.link_peak_bandwidth.values())
    
    def update_link_traffic_pattern(self, 
                             job_id: int, 
                             job_cycle_len: int, 
                             job_demands: list[Demand],
                             job_bw_alloc: dict[int, PathBwAlloc]
                             ) -> None:
    
        passed_link_set: set[SDPair] = set() # 任务经过的所有链路(s-d对)
        for demand_idx, path_bw_alloc in job_bw_alloc.items():
            for path, _ in path_bw_alloc:
                for link in path:
                    if link in passed_link_set:
                        continue
                    passed_link_set.add(link)
                    # 添加流量模式
                    if link not in self.link_traffic_patterns:
                        self.link_traffic_patterns[link] = []
                    pattern = LinkTrafficPattern(
                        job_id = job_id,
                        cycle = job_cycle_len,
                        start_time = job_demands[demand_idx].start_time,
                        end_time = job_demands[demand_idx].end_time, 
                        bandwidth = job_demands[demand_idx].bandwidth
                    )
                    self.link_traffic_patterns[link].append(pattern)

        # 将当前任务经过的链路添加到全局链路集合中
        self.all_links.update(passed_link_set)
            
        for link in self.all_links:
            peak_bw = 0.0
            circle_list = []
            
            # 计算重叠流量周期
            for traffic_pattern in self.link_traffic_patterns[link]:
                rounded_cycle = ((traffic_pattern.cycle + self.circle_precision // 2) // 
                                self.circle_precision) * self.circle_precision
                circle_list.append(rounded_cycle)
            
            if circle_list:
                overlap_circle = min(np.lcm.reduce(circle_list), self.max_overlap_circle)
                
                # 收集所有流量变化点
                change_points = set()
                for pattern in self.link_traffic_patterns[link]:
                    for t in range(0, overlap_circle, pattern.cycle):
                        start = (pattern.start_time + t) % overlap_circle
                        end = (pattern.end_time + t) % overlap_circle
                        change_points.add(start)
                        change_points.add(end)
                
                # 在每个流量变化点计算带宽
                for time in sorted(list(change_points)):
                    bw_this_time = 0.0
                    for pattern in self.link_traffic_patterns[link]:
                        time_in_job = (time + pattern.cycle) % pattern.cycle
                        if time_in_job >= pattern.start_time and time_in_job < pattern.end_time:
                            bw_this_time += pattern.bandwidth
                    peak_bw = max(peak_bw, bw_this_time)
            
            self.link_peak_bandwidth[link] = peak_bw

    def format_results(self) -> list:
        """将调度结果格式化为标准输出格式"""
        formatted_results = []
        for result in self.results:
            job_result = {
                "job_id": result.job_id,
                "status": "success" if result.success else "failed",
                "start_time": result.start_time if result.success else None
            }
            
            if result.success:
                job_result["demands"] = []
                for demand_id, paths in result.paths_allocation.items():
                    paths_info = []
                    for path, bw in paths:
                        path_str = "_".join(str(link[0]) for link in path)
                        paths_info.append({"path": path_str, "bandwidth": bw})
                    job_result["demands"].append({
                        "demand_id": demand_id,
                        "paths": paths_info
                    })
                
            formatted_results.append(job_result)
            
        return formatted_results
