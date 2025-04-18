from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Set
import heapq
from .demand import Demand
from .graph import Graph, Link
from .path_finder import PathFinder
from .scheduler_base import SchedulerBase, ScheduleResult
import numpy as np

@dataclass
class ScheduleState:
    """调度状态"""
    start_time: int
    allocated_bw: Dict[Tuple[int, int], float]  # link -> bandwidth
    total_cost: float
    remaining_demands: List[Demand]

    def __lt__(self, other):
        return self.total_cost < other.total_cost

class AStarScheduler(SchedulerBase):
    def __init__(self, graph: Graph, path_finder: PathFinder):
        super().__init__(graph, path_finder)
        self.max_search_states = 1000  # 最大搜索状态数
        self.link_usage_weight = 0.7  # 链路利用率权重
        self.time_delay_weight = 0.3  # 时间延迟权重
        
        # 初始化链路相关数据
        nodes = list(graph.nodes)  # 获取所有节点
        for src in nodes:
            for dst in nodes:
                if graph.get_link(src, dst) is not None:
                    link_pair = (src, dst)
                    self.all_links.add(link_pair)
                    self.link_traffic_patterns[link_pair] = []

    def _calculate_state_cost(self, state: ScheduleState, goal_demands: List[Demand]) -> float:
        """计算状态代价"""
        # 当前链路利用率
        link_usage_cost = 0.0
        for bw in state.allocated_bw.values():
            link_usage_cost += bw

        # 剩余需求的估计代价
        remaining_cost = 0.0
        for demand in state.remaining_demands:
            paths = self.path_finder.find_all_paths(demand.src_rank, demand.dst_rank)
            if paths:
                # 使用最短路径的代价作为启发式估计
                min_path_cost = float('inf')
                for path in paths:
                    path_cost = sum(link.delay for link in path)
                    min_path_cost = min(min_path_cost, path_cost)
                remaining_cost += min_path_cost * demand.bandwidth

        return (self.link_usage_weight * link_usage_cost + 
                self.time_delay_weight * (state.start_time + remaining_cost))

    def _get_next_states(self, current: ScheduleState, job_cycle: int) -> List[ScheduleState]:
        """生成下一个可能的状态"""
        next_states = []
        
        # 尝试为剩余需求分配带宽
        for i, demand in enumerate(current.remaining_demands):
            paths = self.path_finder.find_all_paths(demand.src_rank, demand.dst_rank)
            
            for path in paths:
                # 检查路径是否可用，确保有足够带宽满足需求
                can_allocate = True
                for link in path:
                    link_key = (link.src_rank, link.dst_rank)
                    used_bw = current.allocated_bw.get(link_key, 0)
                    if link.bandwidth - used_bw < demand.bandwidth:
                        can_allocate = False
                        break

                if can_allocate:
                    # 创建新状态，分配完整的需求带宽
                    new_allocated_bw = dict(current.allocated_bw)
                    for link in path:
                        link_key = (link.src_rank, link.dst_rank)
                        new_allocated_bw[link_key] = new_allocated_bw.get(link_key, 0) + demand.bandwidth

                    new_remaining = current.remaining_demands[:i] + current.remaining_demands[i+1:]
                    
                    # 创建新状态
                    new_state = ScheduleState(
                        start_time=(current.start_time + self.time_precision) % job_cycle,
                        allocated_bw=new_allocated_bw,
                        total_cost=0,  # 临时值，后续更新
                        remaining_demands=new_remaining
                    )
                    next_states.append(new_state)

        return next_states

    def schedule_job(self, job: Dict) -> ScheduleResult:
        job_id = job['job_id']
        demands = []
        
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

        # 初始状态
        initial_state = ScheduleState(0, {}, 0, demands)
        initial_state.total_cost = self._calculate_state_cost(initial_state, demands)
        
        # A*搜索
        open_set = [initial_state]
        closed_set = set()
        
        while open_set and len(closed_set) < self.max_search_states:
            current = heapq.heappop(open_set)
            state_hash = hash(str(current.allocated_bw))
            
            # 如果状态已经访问过，跳过
            if state_hash in closed_set:
                continue
            
            # 检查是否达到目标
            if not current.remaining_demands:
                # 构造路径分配结果
                paths_allocation = {}
                demand_path_found = [False] * len(demands)  # 跟踪每个需求是否找到路径

                # 为每个需求尝试找路径
                for i, demand in enumerate(demands):
                    paths = self.path_finder.find_all_paths(demand.src_rank, demand.dst_rank)
                    
                    for path in paths:
                        path_bw = 0.0
                        # 计算这条路径上实际分配的带宽
                        for link in path:
                            link_key = (link.src_rank, link.dst_rank)
                            if link_key in current.allocated_bw:
                                if path_bw == 0:  # 第一条链路
                                    path_bw = current.allocated_bw[link_key]
                                else:  # 取所有链路中最小的带宽值
                                    path_bw = min(path_bw, current.allocated_bw[link_key])
                        
                        # 如果路径上有足够的带宽
                        if abs(path_bw - demand.bandwidth) < 1e-6:
                            sd_path = [(link.src_rank, link.dst_rank) for link in path]
                            paths_allocation[i] = [[sd_path, demand.bandwidth]]
                            demand_path_found[i] = True
                            break
                
                # 只有当所有需求都找到合适的路径时才返回成功
                if all(demand_path_found):
                    result = ScheduleResult(
                        job_id=job_id,
                        success=True,
                        start_time=current.start_time,
                        paths_allocation=paths_allocation
                    )
                    self.results.append(result)
                    self.update_link_traffic_pattern(job_id, job['cycle(ms)'], demands, paths_allocation)
                    return result

            # 生成下一步状态
            for next_state in self._get_next_states(current, job['cycle(ms)']):
                next_state.total_cost = self._calculate_state_cost(next_state, demands)
                heapq.heappush(open_set, next_state)
            
            closed_set.add(state_hash)

        # 搜索失败
        return ScheduleResult(
            job_id=job_id,
            success=False,
            start_time=0,
            paths_allocation={}
        )