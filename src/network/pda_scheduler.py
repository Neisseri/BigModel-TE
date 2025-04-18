from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Set
from .demand import Demand
from .graph import Graph, Link
from .path_finder import PathFinder
from .scheduler_base import SchedulerBase, ScheduleResult
import numpy as np

@dataclass
class PDAScheduleResult:
    success: bool
    start_time: float  # task start time
    paths_allocation: dict[int, list[tuple]]  # demand_id -> [(path, bandwidth)]
    total_allocated_bandwidth: float
    peak_bandwidth: float  # 调度后的总带宽
    failed_demand: Optional[dict] = None

class PDAScheduler(SchedulerBase):
    def __init__(self, graph: Graph, path_finder: PathFinder):
        super().__init__(graph, path_finder)
        self.max_iterations = 100  # 最大迭代次数
        self.candidates_per_round = 5  # 每轮候选数
        self.results = []

    def _solve_relaxed_problem(self, demands: List[Demand]) -> Tuple[Dict[Tuple[int, int], float], Dict[int, List[Tuple[List[Link], float]]]]:
        """求解松弛问题"""
        # 初始化带宽分配和路径分配
        bandwidth_allocation = {}  # (src, dst) -> bandwidth
        paths_allocation = {}  # demand_id -> [(path, bandwidth)]
        
        for i, demand in enumerate(demands):
            paths = self.path_finder.find_all_paths(demand.src_rank, demand.dst_rank)
            if not paths:
                continue
                
            # 选择最短路径并分配带宽
            shortest_path = min(paths, key=lambda p: sum(link.delay for link in p))
            paths_allocation[i] = [(shortest_path, demand.bandwidth)]
            
            # 更新链路带宽分配
            for link in shortest_path:
                key = (link.src_rank, link.dst_rank)
                bandwidth_allocation[key] = bandwidth_allocation.get(key, 0) + demand.bandwidth
        
        return bandwidth_allocation, paths_allocation

    def _calculate_cost(self, bandwidth_allocation: Dict[Tuple[int, int], float]) -> float:
        """计算带宽分配的总成本"""
        return sum(bw for bw in bandwidth_allocation.values())

    def _select_candidates(self, bandwidth_allocation: Dict[Tuple[int, int], float], K: int) -> List[Tuple[int, int]]:
        """选择K个最接近整数的候选链路"""
        errors = [(link, abs(bw - round(bw))) for link, bw in bandwidth_allocation.items()]
        errors.sort(key=lambda x: x[1])
        return [link for link, _ in errors[:K]]

    def schedule_job(self, job: Dict) -> ScheduleResult:
        job_id = job['job_id']
        demands = []
        
        # 构建需求列表
        for demand_data in job['demands']:
            demand = Demand(
                job_id=job_id,
                src_rank=demand_data['src_rank'],
                dst_rank=demand_data['dst_rank'],
                start_time=demand_data['start_timestamp(ms)'],
                end_time=demand_data['end_timestamp(ms)'],
                bandwidth=demand_data['bandwidth(Gbps)']
            )
            demands.append(demand)

        # Step 1: 求解松弛问题
        bw_alloc, paths_alloc = self._solve_relaxed_problem(demands)
        
        # Step 2: 初始化上界
        best_bw_alloc = {k: np.ceil(v) for k, v in bw_alloc.items()}
        best_cost = self._calculate_cost(best_bw_alloc)
        best_paths = paths_alloc.copy()
        
        # Step 3: 迭代优化
        for _ in range(self.max_iterations):
            improved = False
            candidates = self._select_candidates(bw_alloc, self.candidates_per_round)
            
            for link in candidates:
                temp_bw_alloc = best_bw_alloc.copy()
                temp_bw_alloc[link] = round(bw_alloc[link])
                
                # 检查新分配是否可行
                cost = self._calculate_cost(temp_bw_alloc)
                if cost < best_cost:
                    best_bw_alloc = temp_bw_alloc
                    best_cost = cost
                    improved = True
                    break
            
            if not improved:
                break
        
        # 修改构造调度结果的代码
        # 将分配的路径转换为正确的格式
        formatted_paths = {}
        for i, paths in best_paths.items():
            path_allocs = []
            for path, bw in paths:
                sd_path = [(link.src_rank, link.dst_rank) for link in path]
                path_allocs.append([sd_path, bw])
            formatted_paths[i] = path_allocs
        
        result = ScheduleResult(
            job_id=job_id,
            success=True,
            start_time=0,
            paths_allocation=formatted_paths
        )
        
        self.results.append(result)
        self.update_link_traffic_pattern(job_id, job['cycle(ms)'], demands, formatted_paths)

        return result