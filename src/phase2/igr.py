from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule, Traffic, Tunnel
from gurobipy import Model, GRB
from params import SCHEDULE_INTERVAL
import numpy as np
import time
from typing import Dict, List, Tuple, Set, Any
from collections import defaultdict, Counter
import heapq
import math
from dataclasses import dataclass

# TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
overlap_circle = SCHEDULE_INTERVAL

# 定义组和路径的数据结构
@dataclass
class Path:
    path_id: int
    links: List[int]  # 链路列表
    allocated_bw: float = 0.0  # 分配的带宽
    weight: int = 1  # 权重

@dataclass
class Group:
    group_id: int
    job_id: int
    workload_id: int
    paths: List[Path]  # 该组包含的路径
    demand: float  # 总需求带宽
    allocated_entries: int = 0  # 分配的表项数

class IGR:
    def __init__(self, network: Graph, jobs: dict[int, JobInfo], schedules: dict[int, JobSchedule]):
        self.network = network
        self.jobs = jobs
        self.schedules = schedules

        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 链路所有流量变化的时间点
        self.change_points: dict[int, set[int]] = {} # link_id -> set[int]
        # 链路峰值带宽
        self.link_peak_bw: dict[int, float] = {} # link_id -> peak_bandwidth
        
        # TODO: 算法参数对运算时间的影响
        ''' 旧参数
        self.groups: List[Group] = []  # 流量组
        self.table_size = 4096  # 默认交换机表项容量
        self.min_paths_per_group = 2  # 每组最小路径数，确保路径多样性
        self.max_weight = 100  # 最大权重
        self.oversub_increment = 0.05  # 过度订阅增量
        self.min_ecmp_size = 8  # 最小ECMP组大小
        self.spread_factor = 0.5  # 路径多样性因子（50% spread）
        self.traffic_change_threshold = 0.1  # 触发重新计算的流量变化阈值
        '''
        # IGR算法参数
        self.groups: List[Group] = []  # 流量组
        self.table_size = 4096  # 默认交换机表项容量
        self.min_paths_per_group = 2  # 每组最小路径数，确保路径多样性
        self.max_weight = 200  # 最大权重，增加以提供更细粒度的带宽分配
        self.oversub_increment = 0.1  # 过度订阅增量，加快过度订阅调整速度
        self.min_ecmp_size = 4  # 最小ECMP组大小，减小以更灵活地分配带宽
        self.spread_factor = 0.7  # 路径多样性因子，增加以提高路径利用率
        self.traffic_change_threshold = 0.05  # 降低阈值以更频繁地触发重新计算
        self.max_oversub_factor = 8.0  # 过度订阅上限，允许更积极地利用链路
        
        # 链路利用率追踪
        self.link_utilization: Dict[int, float] = {}
        # 路径组表
        self.path_groups: Dict[int, Dict[int, List[Path]]] = {}  # job_id -> {workload_id -> paths}

    def update_traffic_pattern(self, job_id: int, workload_id: int, new_bw: float):

        tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
        for link in tunnel:
            if link.link_id not in self.link_traffic:
                self.link_traffic[link.link_id] = []
                self.change_points[link.link_id] = set()
            self.link_traffic[link.link_id].append(
                Traffic(
                    job_id = job_id,
                    cycle = self.jobs[job_id].cycle,
                    t_s = self.jobs[job_id].workloads[workload_id].t_s,
                    t_e = self.jobs[job_id].workloads[workload_id].t_e,
                    bw = new_bw
                )
            )
            # 添加新流量的变化时间点
            for circle_offset in range(0, overlap_circle, self.jobs[job_id].cycle):
                start = (self.jobs[job_id].workloads[workload_id].t_s + circle_offset + self.schedules[job_id].start_time) % overlap_circle
                end = (self.jobs[job_id].workloads[workload_id].t_e + circle_offset + self.schedules[job_id].start_time) % overlap_circle
                            
                self.change_points[link.link_id].add(start)
                self.change_points[link.link_id].add(end)

    def calculate_bottleneck_bw(self, tunnel: Tunnel, job_id: int, workload_id: int) -> float:
        
        bottleneck_bw = float("inf")

        for link in tunnel:
            
            cycle = self.jobs[job_id].cycle
            t_s = (self.jobs[job_id].workloads[workload_id].t_s + self.schedules[job_id].start_time) % cycle
            t_e = (self.jobs[job_id].workloads[workload_id].t_e + self.schedules[job_id].start_time) % cycle
            
            link_alloc_bw = 0.0
            if link.link_id not in self.link_traffic:
                self.link_traffic[link.link_id] = []
                self.change_points[link.link_id] = set()

            # 在每个流量变化时间点计算总带宽
            for time in sorted(list(self.change_points[link.link_id])):
                if time % cycle >= t_s and time % cycle < t_e:
                    # 计算当前时间点的带宽
                    bw_now = 0.0
                    for traffic in self.link_traffic[link.link_id]:
                        traffic_job_id = traffic.job_id
                        time_in_circle = (time + traffic.cycle - self.schedules[traffic_job_id].start_time) % traffic.cycle
                        if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                            bw_now += traffic.bw
                    if bw_now >= link_alloc_bw:
                        link_alloc_bw = bw_now

            # print("link_id: ", link.link_id, " link_capacity: ", link.capacity, " link_alloc_bw: ", link_alloc_bw)
            
            link_left_bw = link.capacity - link_alloc_bw
            if link_left_bw < bottleneck_bw:
                bottleneck_bw = link_left_bw

        return bottleneck_bw

    def update_schedule(self) -> tuple[float, float]:

        total_flow = 0.0
        self.link_traffic = {}
        self.change_points = {}
        self.link_peak_bw = {}

        total_workload_bw = 0.0

        for job_id, job in self.jobs.items():

            # 创建 Gurobi 模型
            model = Model("TrafficScheduler")
            model.setParam('OutputFlag', 0)  # 关闭输出日志

            # 添加变量：updated_workload 中每个负载分配的流量大小
            # TODO: 此时每个负载分配一条流，后续需要修改为多隧道
            flow_vars = {}
            for workload_id, workload in enumerate(job.workloads):
                flow_vars[workload_id] = model.addVar(
                    vtype=GRB.CONTINUOUS,
                    name=f"flow_{workload_id}",
                    lb=0.0,
                    ub=float("inf")
                )
                total_workload_bw += workload.bw

            # 设置目标函数：最大化总流量
            model.setObjective(
                sum(flow_vars[workload_id] for workload_id in range(len(job.workloads))),
                GRB.MAXIMIZE
            )

            # 链路容量约束
            # TODO: 为了简化，不考虑更新流之间的重叠（即每个约束只有一个变量）
            # 可以通过减小数据集中更新的负载数来降低这个简化的负面效果，后续再修改
            for workload_id, workload in enumerate(job.workloads):
                tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]

                # 计算瓶颈带宽
                bottleneck_bw = self.calculate_bottleneck_bw(tunnel, job_id, workload_id)
                if bottleneck_bw < 0:
                    bottleneck_bw = 0

                model.addConstr(
                    flow_vars[workload_id] <= bottleneck_bw,
                    name=f"link_capacity_{workload_id}"
                )

            # 带宽需求约束
            for workload_id, workload in enumerate(job.workloads):
                model.addConstr(
                    flow_vars[workload_id] <= workload.bw,
                    name=f"bw_demand_{workload_id}"
                )

            # 求解模型
            model.optimize()

            # 检查是否找到可行解
            if model.status in [GRB.OPTIMAL, GRB.SUBOPTIMAL, GRB.TIME_LIMIT, GRB.SOLUTION_LIMIT]:
                for workload_id in range(len(job.workloads)):
                    total_flow += flow_vars[workload_id].X
                    self.update_traffic_pattern(job_id, workload_id, flow_vars[workload_id].X)
            else:
                raise ValueError("Gurobi failed to find a feasible solution.")
            
            model.dispose()

        return total_flow, total_workload_bw
    
    def calculate_peak_bw(self, link_id: int):
        """计算链路峰值带宽和利用率"""
        if link_id not in self.link_traffic:
            self.link_traffic[link_id] = []
            self.change_points[link_id] = set()

        peak_bw = 0.0
        # 在每个流量变化时间点计算总带宽
        for time in sorted(list(self.change_points[link_id])):
            bw_now = 0.0
            for traffic in self.link_traffic[link_id]:
                time_in_circle = (time + traffic.cycle - self.schedules[traffic.job_id].start_time) % traffic.cycle
                if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                    bw_now += traffic.bw
            if bw_now >= peak_bw:
                peak_bw = bw_now
                
        self.link_peak_bw[link_id] = peak_bw
        
        # 计算链路利用率
        link_capacity = 0.0
        for src_node, links in self.network.edges.items():
            for link in links:
                if link.link_id == link_id:
                    link_capacity = link.capacity
                    break
            if link_capacity > 0:
                break
                
        if link_capacity > 0:
            self.link_utilization[link_id] = peak_bw / link_capacity
            
    def initialize_groups(self):
        """初始化流量组，为每个工作负载创建路径组"""
        group_id = 0
        for job_id, job in self.jobs.items():
            for workload_id, workload in enumerate(job.workloads):
                # 获取该工作负载的隧道
                tunnel = self.schedules[job_id].tunnels[workload_id]
                
                # 获取隧道中的链路ID列表
                links = [link.link_id for link in tunnel]
                
                # 创建默认路径
                default_path = Path(
                    path_id=0,
                    links=links,
                    allocated_bw=0.0,
                    weight=1
                )
                
                # 创建组
                group = Group(
                    group_id=group_id,
                    job_id=job_id,
                    workload_id=workload_id,
                    paths=[default_path],
                    demand=workload.bw
                )
                
                self.groups.append(group)
                
                # 存储到路径组表
                if job_id not in self.path_groups:
                    self.path_groups[job_id] = {}
                self.path_groups[job_id][workload_id] = [default_path]
                
                group_id += 1
                
    def traffic_proportional_allocation(self, group: Group) -> int:
        """基于流量需求比例分配表项"""
        total_demand = sum(g.demand for g in self.groups)
        if total_demand == 0:
            return self.min_ecmp_size
            
        # 基于流量需求的占比分配表空间
        allocated = int(self.table_size * (group.demand / total_demand))
        return max(self.min_ecmp_size, allocated)
    
    def table_carving(self):
        """第一步：表空间分配"""
        for group in self.groups:
            group.allocated_entries = self.traffic_proportional_allocation(group)
            
    def max_oversub(self, paths: List[Path]) -> float:
        """计算路径集合的最大过度订阅比例"""
        link_loads = defaultdict(float)
        
        # 计算每条链路的负载
        for path in paths:
            for link_id in path.links:
                link_loads[link_id] += path.allocated_bw
        
        max_ratio = 0.0
        for link_id, load in link_loads.items():
            # 查找链路容量
            capacity = 0.0
            for src_node, links in self.network.edges.items():
                for link in links:
                    if link.link_id == link_id:
                        capacity = link.capacity
                        break
                if capacity > 0:
                    break
                    
            if capacity > 0:
                ratio = load / capacity
                max_ratio = max(max_ratio, ratio)
                
        return max_ratio
        
    def adjust_weights(self, group: Group, paths: List[Path], max_weight: int) -> List[Path]:
        """调整路径权重以满足表项和过度订阅约束"""
        # 先按链路利用率排序，优先使用利用率低的路径
        path_scores = []
        for path in paths:
            # 计算路径的平均链路利用率分数
            util_score = 0
            count = 0
            for link_id in path.links:
                if link_id in self.link_utilization:
                    util_score += self.link_utilization[link_id]
                    count += 1
            avg_util = util_score / max(1, count)
            
            # 计算路径分数 (负相关，利用率越低分数越高)
            path_score = 1.0 - min(1.0, avg_util)
            path_scores.append((path, path_score))
        
        # 基于负载和利用率综合排序 (70% 带宽, 30% 链路利用率)
        sorted_paths = [p[0] for p in sorted(
            [(p, 0.7 * (p.allocated_bw / max(0.001, group.demand)) + 0.3 * score) 
             for p, score in path_scores],
            key=lambda x: x[1],
            reverse=True
        )]
        
        # 尝试分配权重
        total_weight = 0
        result_paths = []
        
        for path in sorted_paths:
            # 计算比例权重，使用更细粒度的权重计算
            if path.allocated_bw > 0:
                # 使用平方根函数使得权重分配更加均匀
                weight_ratio = (path.allocated_bw / group.demand) ** 0.5
                weight = min(max_weight, max(1, int(weight_ratio * max_weight)))
            else:
                weight = 1  # 确保至少有1的权重
                
            path.weight = weight
            total_weight += weight
            result_paths.append(path)
            
            # 检查是否超过分配的表项数
            if total_weight > group.allocated_entries:
                # 优化缩减因子
                scale_factor = (group.allocated_entries / total_weight) ** 1.1
                
                # 重新分配权重
                new_total = 0
                for p in result_paths:
                    p.weight = max(1, int(p.weight * scale_factor))
                    new_total += p.weight
                    
                # 调整到精确匹配
                while new_total > group.allocated_entries:
                    # 按权重和带宽比值排序，减少效率低的路径权重
                    sorted_idx = sorted(
                        range(len(result_paths)), 
                        key=lambda i: result_paths[i].allocated_bw / max(1, result_paths[i].weight), 
                        reverse=False
                    )
                    
                    for idx in sorted_idx:
                        if result_paths[idx].weight > 1:
                            result_paths[idx].weight -= 1
                            new_total -= 1
                            break
                    
                    if all(p.weight <= 1 for p in result_paths):
                        break  # 无法再减少
                        
                while new_total < group.allocated_entries:
                    # 按权重和带宽比值排序，增加效率高的路径权重
                    sorted_idx = sorted(
                        range(len(result_paths)), 
                        key=lambda i: result_paths[i].allocated_bw / max(1, result_paths[i].weight),
                        reverse=True
                    )
                    
                    for idx in sorted_idx:
                        result_paths[idx].weight += 1
                        new_total += 1
                        break
                    
                break
                
        return result_paths
    
    def meets_constraints(self, paths: List[Path], theta: float) -> bool:
        """检查路径集合是否满足约束条件"""
        # 确保路径数量满足最小多样性要求
        if len(paths) < self.min_paths_per_group:
            return False
            
        # 检查最大过度订阅约束，但允许一定程度的超额
        max_oversub = self.max_oversub(paths)
        
        # 对不同链路利用率范围设置不同容忍度
        tolerance_factor = 1.0
        path_links = set()
        for path in paths:
            for link_id in path.links:
                path_links.add(link_id)
                
        # 计算平均链路利用率
        avg_utilization = 0
        count = 0
        for link_id in path_links:
            if link_id in self.link_utilization:
                avg_utilization += self.link_utilization[link_id]
                count += 1
                
        if count > 0:
            avg_utilization /= count
            # 对低负载链路更宽松
            if avg_utilization < 0.3:
                tolerance_factor = 1.5
            elif avg_utilization < 0.6:
                tolerance_factor = 1.2
        
        # 应用动态容忍度
        adjusted_theta = theta * tolerance_factor
                
        if max_oversub > adjusted_theta:
            return False
            
        return True
        
    def reduce_single_group(self, group: Group, theta: float) -> List[Path]:
        """减少单个组的路径和权重，满足约束条件"""
        # 获取当前路径集合
        paths = self.path_groups.get(group.job_id, {}).get(group.workload_id, [])
        
        if not paths:
            # 如果没有路径，返回空列表
            return []
        
        # 确保至少有最小数量的路径
        if len(paths) < self.min_paths_per_group:
            # 尝试添加更多路径，但是考虑链路利用率
            existing_links = set()
            for path in paths:
                for link_id in path.links:
                    existing_links.add(link_id)
                    
            # 从原始隧道中获取链路ID列表
            tunnel = self.schedules[group.job_id].tunnels[group.workload_id]
            tunnel_links = [link.link_id for link in tunnel]
            
            # 尝试为新路径找到较低利用率的链路组合
            while len(paths) < self.min_paths_per_group:
                # 简单实现：复制现有路径，但尝试避免高利用率链路
                base_path = paths[0]  # 基础路径
                
                # 创建新路径
                new_path = Path(
                    path_id=len(paths),
                    links=base_path.links.copy(),  # 复制链路列表
                    allocated_bw=0.0,
                    weight=1
                )
                
                # 尝试用较低利用率的链路替代高利用率链路
                high_util_links = {}
                for link_id in new_path.links:
                    if link_id in self.link_utilization and self.link_utilization[link_id] > 0.7:
                        high_util_links[link_id] = self.link_utilization[link_id]
                
                # 添加新路径
                paths.append(new_path)
        
        # 计算当前配置
        best_paths = paths.copy()
        best_total_weight = sum(p.weight for p in best_paths)
        best_total_bw = sum(p.allocated_bw for p in best_paths)
        
        # 如果当前配置已经满足约束，直接返回
        if (best_total_weight <= group.allocated_entries and 
            self.meets_constraints(best_paths, theta)):
            return best_paths
        
        # 尝试不同的权重配置 - 采用二分搜索加快搜索过程
        low_w = 1
        high_w = self.max_weight
        
        # 二分搜索最佳权重配置
        while low_w <= high_w:
            mid_w = (low_w + high_w) // 2
            
            # 尝试当前权重配置
            trial_paths = self.adjust_weights(group, paths.copy(), mid_w)
            
            # 检查是否满足约束
            total_weight = sum(p.weight for p in trial_paths)
            meets_constraints = (total_weight <= group.allocated_entries and
                                self.meets_constraints(trial_paths, theta))
            
            if meets_constraints:
                # 如果满足约束，尝试更高权重
                low_w = mid_w + 1
                
                # 更新最佳配置
                total_bw = sum(p.allocated_bw for p in trial_paths)
                if total_bw > best_total_bw:
                    best_paths = trial_paths
                    best_total_bw = total_bw
            else:
                # 如果不满足约束，尝试更低权重
                high_w = mid_w - 1
        
        # 如果无法找到满足约束的配置，尝试渐进式降低要求
        if sum(p.weight for p in best_paths) > group.allocated_entries:
            # 1. 先尝试自适应ECMP
            sorted_paths = sorted(paths, key=lambda p: p.allocated_bw, reverse=True)
            
            # 根据带宽需求比例分配权重
            total_demand = sum(p.allocated_bw for p in sorted_paths)
            if total_demand > 0:
                # 初始分配
                for path in sorted_paths:
                    ratio = path.allocated_bw / total_demand if total_demand > 0 else 1.0 / len(sorted_paths)
                    path.weight = max(1, int(ratio * group.allocated_entries * 0.9))  # 预留10%空间用于调整
                
                # 微调以满足表项限制
                total_weight = sum(p.weight for p in sorted_paths)
                if total_weight > group.allocated_entries:
                    scale = group.allocated_entries / total_weight
                    for path in sorted_paths:
                        path.weight = max(1, int(path.weight * scale))
                
                # 检查约束
                if self.meets_constraints(sorted_paths, theta * 1.1):  # 略微放宽theta
                    best_paths = sorted_paths
                else:
                    # 2. 如果仍然不满足，降级为基本ECMP
                    equal_weight = max(1, group.allocated_entries // len(paths))
                    remainder = group.allocated_entries % len(paths)
                    
                    for i, path in enumerate(paths):
                        path.weight = equal_weight + (1 if i < remainder else 0)
                    
                    # 再次检查约束
                    if self.meets_constraints(paths, theta * 1.2):  # 进一步放宽
                        best_paths = paths
        
        return best_paths
        
    def greedy_alloc(self) -> tuple[float, float]:
        """简单的贪心带宽分配方法"""
        total_flow = 0.0
        total_workload_bw = 0.0

        for link_id in range(self.network.link_num):
            self.calculate_peak_bw(link_id)

        for job_id, job in self.jobs.items():

            update_link_id: set[int] = set()
            for workload_id, workload in enumerate(job.workloads):
                tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
                bottleneck_bw = float("inf")
                for link in tunnel:
                    bottleneck_bw = min(bottleneck_bw, self.link_peak_bw[link.link_id])
                alloc_bw = min(workload.bw, bottleneck_bw)
                
                for link in tunnel:
                    self.update_traffic_pattern(job_id, workload_id, alloc_bw)
                    self.link_peak_bw[link.link_id] -= alloc_bw
                    
                total_flow += alloc_bw
                total_workload_bw += workload.bw
            
            for link_id in update_link_id:
                self.calculate_peak_bw(link_id)

        return total_flow, total_workload_bw
        
    def igr_alloc(self) -> tuple[float, float]:
        """使用IGR算法分配带宽"""
        # 初始化数据结构
        self.groups = []
        self.path_groups = {}
        total_flow = 0.0
        total_workload_bw = 0.0
        
        # 步骤1：初始化所有链路带宽
        for link_id in range(self.network.link_num):
            self.calculate_peak_bw(link_id)
        
        # 步骤2：初始化流量组
        self.initialize_groups()
        
        # 步骤3：计算总工作负载带宽需求
        for job_id, job in self.jobs.items():
            for workload in job.workloads:
                total_workload_bw += workload.bw
        
        # 步骤4：表空间分配
        self.table_carving()
        
        # 步骤5：迭代降低过度订阅，优化路径权重
        theta = 1.2  # 初始过度订阅限制，略微放宽初始值
        changed = True
        
        # 计算所有组的分配表项总数
        total_entries = sum(group.allocated_entries for group in self.groups)
        
        # 如果分配表项超过表大小，需要减少
        max_iterations = 15  # 增加迭代次数以找到更优解
        iteration_count = 0
        best_allocation = None
        best_total_bw = 0
        
        # 优先处理高优先级的流量组，预留足够的表项
        high_priority_groups = []
        normal_priority_groups = []
        reserved_entries = 0
        
        # 根据需求和紧急程度将组分为高优先级和普通优先级
        for group in self.groups:
            job_id = group.job_id
            workload_id = group.workload_id
            workload = self.jobs[job_id].workloads[workload_id]
            
            # 简单启发式：如果需求占总需求比例大于10%，视为高优先级
            if group.demand > 0.1 * sum(g.demand for g in self.groups):
                high_priority_groups.append(group)
                reserved_entries += min(self.min_ecmp_size * 2, int(self.table_size * 0.05))  # 为每个高优先级预留表项
            else:
                normal_priority_groups.append(group)
        
        available_entries = max(0, self.table_size - reserved_entries)
        
        while total_entries > self.table_size and changed and iteration_count < max_iterations:
            changed = False
            iteration_count += 1
            
            # 首先处理普通优先级组
            # 按需求大小逆序排序流量组
            sorted_normal_groups = sorted(normal_priority_groups, key=lambda g: g.demand, reverse=True)
            
            for group in sorted_normal_groups:
                # 减少单个组的配置
                reduced_paths = self.reduce_single_group(group, theta)
                
                # 如果配置有变化
                current_paths = self.path_groups.get(group.job_id, {}).get(group.workload_id, [])
                if sum(p.weight for p in reduced_paths) < sum(p.weight for p in current_paths):
                    # 更新配置
                    self.path_groups[group.job_id][group.workload_id] = reduced_paths
                    changed = True
            
            # 然后处理高优先级组（更保守地减少它们的配置）
            sorted_high_groups = sorted(high_priority_groups, key=lambda g: g.demand, reverse=False)
            
            # 如果普通组的调整不足，才调整高优先级组
            if not changed and total_entries > self.table_size:
                for group in sorted_high_groups:
                    # 使用较小的theta值，使高优先级流量保持更稳定
                    reduced_paths = self.reduce_single_group(group, max(1.0, theta * 0.8)) 
                    
                    # 如果配置有变化
                    current_paths = self.path_groups.get(group.job_id, {}).get(group.workload_id, [])
                    if sum(p.weight for p in reduced_paths) < sum(p.weight for p in current_paths):
                        # 更新配置
                        self.path_groups[group.job_id][group.workload_id] = reduced_paths
                        changed = True
            
            # 保存当前最佳分配情况
            current_bw_estimate = 0
            for group in self.groups:
                job_id = group.job_id
                workload_id = group.workload_id
                paths = self.path_groups.get(job_id, {}).get(workload_id, [])
                current_bw_estimate += sum(p.allocated_bw for p in paths)
            
            if current_bw_estimate > best_total_bw:
                best_total_bw = current_bw_estimate
                best_allocation = {g.job_id: {g.workload_id: self.path_groups.get(g.job_id, {}).get(g.workload_id, [])[:] 
                                  for g in self.groups}}
            
            # 如果无法进一步减少，放宽过度订阅限制
            if not changed and theta < self.max_oversub_factor:
                theta += self.oversub_increment
                # 让theta增长越来越慢，避免过度订阅
                self.oversub_increment = max(0.02, self.oversub_increment * 0.9)
                changed = True  # 尝试更宽松的约束
            else:
                # 如果theta已经很大或者有变化，重新计算总表项
                total_entries = sum(sum(path.weight for path in 
                                    self.path_groups.get(group.job_id, {}).get(group.workload_id, [])) 
                                    for group in self.groups)
        
        # 如果找到了更好的分配方案，恢复它
        if best_allocation and iteration_count >= max_iterations:
            for group in self.groups:
                job_id = group.job_id
                workload_id = group.workload_id
                if job_id in best_allocation and workload_id in best_allocation[job_id]:
                    self.path_groups[job_id][workload_id] = best_allocation[job_id][workload_id]
        
        # 步骤6：基于路径权重分配带宽
        # 先计算每条链路的可用带宽
        link_available_bw = {}
        oversub_factor = min(1.2, theta if theta > 1.0 else 1.0)  # 安全过度订阅因子
        for src_node, links in self.network.edges.items():
            for link in links:
                # 对容量进行轻微过度订阅，以提高利用率
                link_available_bw[link.link_id] = link.capacity * oversub_factor
        
        # 按优先级处理组，先处理高优先级小工作负载
        # 1. 小作业先满足其最低需求
        # 2. 大作业分配剩余带宽
        
        # 多阶段分配策略：先保证最低需求，再分配额外带宽
        sorted_groups = sorted(self.groups, key=lambda g: g.demand)
        
        # 第一阶段：为每个组分配最低保证带宽 (每个组需求的30%)
        min_guarantee_pct = 0.3
        for group in sorted_groups:
            job_id = group.job_id
            workload_id = group.workload_id
            workload = self.jobs[job_id].workloads[workload_id]
            paths = self.path_groups.get(job_id, {}).get(workload_id, [])
            
            if not paths:
                continue
                
            # 计算总权重
            total_weight = sum(path.weight for path in paths)
            if total_weight == 0:
                continue
            
            # 最低保证带宽
            min_guarantee = group.demand * min_guarantee_pct
            
            # 为每条路径分配最低保证带宽
            for path in paths:
                # 按权重比例分配最低保证带宽
                path_min_bw = (path.weight / total_weight) * min_guarantee
                
                # 检查路径上的可用带宽
                path_available_bw = float("inf")
                for link_id in path.links:
                    path_available_bw = min(path_available_bw, link_available_bw.get(link_id, 0))
                
                # 分配实际带宽，不超过路径可用带宽
                allocated_bw = min(path_min_bw, path_available_bw)
                path.allocated_bw = allocated_bw
                
                # 更新链路可用带宽
                if allocated_bw > 0:
                    for link_id in path.links:
                        link_available_bw[link_id] = max(0, link_available_bw[link_id] - allocated_bw)
                    
                    # 累计总流量但不更新流量模式，等所有分配完成再更新
                    total_flow += allocated_bw
        
        # 第二阶段：分配剩余带宽，优先给小工作负载
        for group in sorted_groups:
            job_id = group.job_id
            workload_id = group.workload_id
            workload = self.jobs[job_id].workloads[workload_id]
            paths = self.path_groups.get(job_id, {}).get(workload_id, [])
            
            if not paths:
                continue
                
            # 计算总权重
            total_weight = sum(path.weight for path in paths)
            if total_weight == 0:
                continue
            
            # 计算已分配的带宽和剩余需求
            already_allocated = sum(path.allocated_bw for path in paths)
            remaining_demand = max(0, group.demand - already_allocated)
            
            if remaining_demand <= 0:
                continue
            
            # 计算每条路径可分配的额外带宽
            path_allocated_extras = {}
            for path in paths:
                path_weight_ratio = path.weight / total_weight
                target_extra_bw = path_weight_ratio * remaining_demand
                
                # 检查路径瓶颈带宽
                path_bottleneck_bw = float("inf")
                for link_id in path.links:
                    path_bottleneck_bw = min(path_bottleneck_bw, link_available_bw.get(link_id, 0))
                
                # 分配可能的额外带宽
                path_allocated_extras[path.path_id] = min(target_extra_bw, path_bottleneck_bw)
            
            # 实际更新路径和链路带宽
            group_additional_bw = 0
            for path in paths:
                extra_bw = path_allocated_extras.get(path.path_id, 0)
                if extra_bw > 0:
                    path.allocated_bw += extra_bw
                    group_additional_bw += extra_bw
                    
                    # 更新链路可用带宽
                    for link_id in path.links:
                        link_available_bw[link_id] = max(0, link_available_bw[link_id] - extra_bw)
                
            # 累计总流量
            total_flow += group_additional_bw
        
        # 第三阶段：应用流量模式更新和链路利用率计算
        for group in self.groups:
            job_id = group.job_id
            workload_id = group.workload_id
            paths = self.path_groups.get(job_id, {}).get(workload_id, [])
            
            # 每条路径更新流量模式
            for path in paths:
                if path.allocated_bw > 0:
                    self.update_traffic_pattern(job_id, workload_id, path.allocated_bw)
            
            # 更新链路利用率
            for path in paths:
                for link_id in path.links:
                    self.calculate_peak_bw(link_id)
        
        # print(f"IGR allocated flow: {total_flow}, total workload demand: {total_workload_bw}")
        return total_flow, total_workload_bw
        
    def schedule(self) -> tuple[float, float]:
        """主调度方法，根据设置选择不同的调度算法"""
        try:
            # 尝试使用IGR算法
            start_time = time.time()
            igr_flow, total_workload_bw = self.igr_alloc()
            igr_time = time.time() - start_time
            
            # 如果IGR分配的流量太低（低于需求的50%），尝试贪心算法作为比较
            if igr_flow < 0.5 * total_workload_bw:
                # 保存IGR结果
                igr_path_groups = self.path_groups.copy()
                igr_link_traffic = self.link_traffic.copy()
                igr_change_points = self.change_points.copy()
                igr_link_peak_bw = self.link_peak_bw.copy()
                
                # 尝试贪心算法
                try:
                    # 重置状态
                    self.link_traffic = {}
                    self.change_points = {}
                    self.link_peak_bw = {}
                    self.path_groups = {}
                    
                    greedy_start = time.time()
                    greedy_flow, _ = self.greedy_alloc()
                    greedy_time = time.time() - greedy_start
                    
                    # 选择更好的结果
                    if greedy_flow > igr_flow * 1.1:  # 如果贪心结果明显更好（10%以上），使用贪心结果
                        # print(f"贪心算法流量更高: {greedy_flow:.2f} > {igr_flow:.2f}, 使用贪心结果")
                        return greedy_flow, total_workload_bw
                    else:
                        # 恢复IGR结果
                        self.path_groups = igr_path_groups
                        self.link_traffic = igr_link_traffic
                        self.change_points = igr_change_points
                        self.link_peak_bw = igr_link_peak_bw
                        return igr_flow, total_workload_bw
                except Exception:
                    # 恢复IGR结果
                    self.path_groups = igr_path_groups
                    self.link_traffic = igr_link_traffic
                    self.change_points = igr_change_points
                    self.link_peak_bw = igr_link_peak_bw
                    return igr_flow, total_workload_bw
            
            # print(f"IGR算法完成，耗时: {igr_time:.3f}秒, 分配流量: {igr_flow:.2f}/{total_workload_bw:.2f}")
            return igr_flow, total_workload_bw
            
        except Exception as e:
            # 如果IGR算法失败，降级使用贪心算法
            # print(f"IGR算法失败: {str(e)}，降级使用贪心算法")
            self.link_traffic = {}  # 重置状态
            self.change_points = {}
            self.link_peak_bw = {}
            return self.greedy_alloc()