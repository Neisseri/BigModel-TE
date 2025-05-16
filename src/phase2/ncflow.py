from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule, Traffic, Tunnel
from gurobipy import Model, GRB
from params import SCHEDULE_INTERVAL
import numpy as np
import heapq
from typing import List, Dict, Tuple, Set, Optional

# TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
overlap_circle = SCHEDULE_INTERVAL

class NCFlow:
    def __init__(self, network: Graph, jobs: dict[int, JobInfo], schedules: dict[int, JobSchedule]):
        self.network = network
        self.jobs = jobs
        self.schedules = schedules

        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 链路所有流量变化的时间点
        self.change_points: dict[int, set[int]] = {} # link_id -> set[int]
        # 链路峰值带宽
        self.link_peak_bw: dict[int, float] = {} # link_id -> peak_bandwidth
        
        # 增加链路利用率追踪
        self.link_utilization: dict[int, float] = {}  # 记录每条链路的当前利用率
        # 链路历史负载记录，用于负载均衡决策
        self.link_history: dict[int, List[float]] = {}  # 链路的历史负载记录
        # 任务优先级，用于动态调整资源分配
        self.job_priorities: dict[int, float] = {}  # 任务的优先级
        # TE调整阈值，用于减少不必要的调整
        self.te_adjust_threshold = 0.1  # 只有当链路利用率变化超过阈值时才进行TE调整

    def update_traffic_pattern(self, job_id: int, workload_id: int, new_bw: float):
        """更新链路流量模式，确保不重复添加相同的流量模式"""
        if new_bw <= 0:
            print(f"警告: 尝试添加零带宽流量 job_id={job_id}, workload_id={workload_id}")
            return

        tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
        t_s = self.jobs[job_id].workloads[workload_id].t_s
        t_e = self.jobs[job_id].workloads[workload_id].t_e
        cycle = self.jobs[job_id].cycle
        
        # 创建流量对象
        traffic = Traffic(
            job_id=job_id,
            cycle=cycle,
            t_s=t_s,
            t_e=t_e,
            bw=new_bw
        )
        
        # 为隧道中的每条链路添加流量模式
        for link in tunnel:
            link_id = link.link_id
            
            # 确保链路数据结构已初始化
            if link_id not in self.link_traffic:
                self.link_traffic[link_id] = []
            if link_id not in self.change_points:
                self.change_points[link_id] = set()
                
            # 检查是否已经存在相同的流量模式
            duplicate_found = False
            for existing_traffic in self.link_traffic[link_id]:
                if (existing_traffic.job_id == job_id and 
                    existing_traffic.t_s == t_s and 
                    existing_traffic.t_e == t_e):
                    # 找到重复记录，更新带宽而不是添加新记录
                    # print(f"更新现有流量记录: job_id={job_id}, link_id={link_id}, old_bw={existing_traffic.bw:.2f}, new_bw={new_bw:.2f}")
                    existing_traffic.bw = new_bw
                    duplicate_found = True
                    break
            
            # 如果没有找到重复记录，添加新的流量记录
            if not duplicate_found:
                self.link_traffic[link_id].append(traffic)
                # print(f"添加新流量记录: job_id={job_id}, link_id={link_id}, bw={new_bw:.2f}")
                
                # 添加新流量的变化时间点
                for circle_offset in range(0, overlap_circle, cycle):
                    start = (t_s + circle_offset + self.schedules[job_id].start_time) % overlap_circle
                    end = (t_e + circle_offset + self.schedules[job_id].start_time) % overlap_circle
                    
                    self.change_points[link_id].add(start)
                    self.change_points[link_id].add(end)

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

    def update_schedule(self) -> float:
        """改进版的流量调度算法，增加动态调整和负载均衡"""
        total_flow = 0.0
        self.link_traffic = {}
        self.change_points = {}
        self.link_peak_bw = {}
        self.link_utilization = {}
        
        # 更新任务优先级
        self.update_job_priorities()
        
        # 按优先级排序任务
        sorted_jobs = sorted(
            self.jobs.items(), 
            key=lambda x: self.job_priorities.get(x[0], 0.0), 
            reverse=True
        )
        
        # 第一阶段：为高优先级任务分配资源
        for job_id, job in sorted_jobs:
            # 创建 Gurobi 模型
            model = Model("TrafficScheduler")
            model.setParam('OutputFlag', 0)  # 关闭输出日志
            model.setParam('TimeLimit', 2)   # 设置求解时间限制，减少计算开销

            # 添加变量：每个负载分配的流量大小
            flow_vars = {}
            for workload_id, workload in enumerate(job.workloads):
                flow_vars[workload_id] = model.addVar(
                    vtype=GRB.CONTINUOUS,
                    name=f"flow_{workload_id}",
                    lb=0.0,
                    ub=float("inf")
                )
            
            # 获取所有相关链路
            relevant_links = set()
            for workload_id, workload in enumerate(job.workloads):
                tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
                for link in tunnel:
                    relevant_links.add(link.link_id)
            
            # 计算链路负载均衡因子
            link_balance_factors = {link_id: self.get_link_balance_factor(link_id) for link_id in relevant_links}
            
            # 构建目标函数：最大化总流量，同时考虑负载均衡
            obj_expr = 0
            for workload_id, workload in enumerate(job.workloads):
                # 基础流量目标
                obj_expr += flow_vars[workload_id]
                
                # 考虑链路负载均衡的惩罚项
                tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
                for link in tunnel:
                    balance_factor = link_balance_factors.get(link.link_id, 0.0)
                    # 负载不均衡的链路产生惩罚，减少其流量分配
                    obj_expr -= 0.1 * balance_factor * flow_vars[workload_id] / len(tunnel)
            
            model.setObjective(obj_expr, GRB.MAXIMIZE)

            # 链路容量约束与负载均衡
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
                # 基于优先级设置最低保证带宽比例
                min_guarantee = 0.5  # 基础保证率
                priority = self.job_priorities.get(job_id, 0.0)
                adjusted_min_bw = workload.bw * min_guarantee * (1 + priority)
                
                # 添加最低保证带宽约束
                model.addConstr(
                    flow_vars[workload_id] >= min(adjusted_min_bw, workload.bw * 0.2), 
                    name=f"min_guarantee_{workload_id}"
                )
                
                # 添加最大需求约束
                model.addConstr(
                    flow_vars[workload_id] <= workload.bw,
                    name=f"bw_demand_{workload_id}"
                )

            # 求解模型
            model.optimize()

            # 检查是否找到可行解
            if model.status in [GRB.OPTIMAL, GRB.SUBOPTIMAL, GRB.TIME_LIMIT, GRB.SOLUTION_LIMIT]:
                for workload_id in range(len(job.workloads)):
                    alloc_bw = flow_vars[workload_id].X
                    total_flow += alloc_bw
                    self.update_traffic_pattern(job_id, workload_id, alloc_bw)
            else:
                # 如果求解失败，尝试退化策略：分配最小带宽
                for workload_id, workload in enumerate(job.workloads):
                    tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
                    # 计算可用的最小带宽
                    bottleneck_bw = self.calculate_bottleneck_bw(tunnel, job_id, workload_id)
                    min_bw = min(workload.bw * 0.2, max(0, bottleneck_bw))
                    if min_bw > 0:
                        total_flow += min_bw
                        self.update_traffic_pattern(job_id, workload_id, min_bw)
            
            model.dispose()
            
            # 在处理每个任务后更新瓶颈链路状态
            bottlenecks = self.get_bottleneck_links(0.9)
            if bottlenecks:
                # 如果有严重瓶颈，更新相关链路的峰值带宽
                for link_id in bottlenecks:
                    self.calculate_peak_bw(link_id)

        # 第二阶段：检查链路利用率是否均衡，必要时进行微调
        bottlenecks = self.get_bottleneck_links(0.95)  # 找出高负载链路
        if bottlenecks:
            for link_id in bottlenecks:
                # 找出使用此链路的所有工作负载
                affected_workloads = []
                for job_id, job_schedule in self.schedules.items():
                    for workload_id, tunnel in enumerate(job_schedule.tunnels):
                        if any(link.link_id == link_id for link in tunnel):
                            affected_workloads.append((job_id, workload_id))
                
                # 按优先级从低到高排序受影响的工作负载
                affected_workloads.sort(key=lambda x: self.job_priorities.get(x[0], 0.0))
                
                # 尝试减少低优先级工作负载的带宽以缓解瓶颈
                for job_id, workload_id in affected_workloads[:max(1, len(affected_workloads)//2)]:
                    # 找到此工作负载在链路上的流量
                    for traffic in self.link_traffic.get(link_id, []):
                        if traffic.job_id == job_id:
                            # 减少10%的带宽
                            reduced_bw = traffic.bw * 0.9
                            if reduced_bw > 0:
                                # 更新流量，并调整总流量计数
                                total_flow -= traffic.bw - reduced_bw
                                traffic.bw = reduced_bw

        print("TE Total flow: ", total_flow)
        return total_flow
    
    def calculate_peak_bw(self, link_id: int):
        
        if link_id not in self.link_traffic:
            self.link_traffic[link_id] = []
            self.change_points[link_id] = set()
            self.link_history[link_id] = []

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
                
        # 查找链路容量
        link_capacity = 0
        # 从所有 edges 中找到对应 link_id 的链路
        for src_node, links in self.network.edges.items():
            for link in links:
                if link.link_id == link_id:
                    link_capacity = link.capacity
                    break
            if link_capacity > 0:
                break
        
        # 如果找不到链路容量，使用默认值或探测网络中的最大链路容量
        if link_capacity <= 0:
            # 探测网络中的最大链路容量作为默认值
            for src, links in self.network.edges.items():
                for link in links:
                    if link.capacity > link_capacity:
                        link_capacity = link.capacity
            
            # 如果仍然为0，设置一个默认值
            if link_capacity <= 0:
                link_capacity = 100.0  # 默认100Gbps
        
        # 更新链路峰值带宽和利用率
        # 计算当前链路剩余可用带宽，不是已用带宽
        available_bw = link_capacity - peak_bw
        # 确保可用带宽不为负
        available_bw = max(0, available_bw)
        self.link_peak_bw[link_id] = available_bw
        
        # 记录链路利用率
        if link_capacity > 0:
            util = peak_bw / link_capacity
            self.link_utilization[link_id] = util
            
            # 记录历史数据，最多保留10条记录
            if link_id not in self.link_history:
                self.link_history[link_id] = []
            self.link_history[link_id].append(util)
            if len(self.link_history[link_id]) > 10:
                self.link_history[link_id].pop(0)
    
    def update_job_priorities(self):
        """更新任务优先级，根据任务大小、等待时间等因素"""
        for job_id, job in self.jobs.items():
            # 基于工作负载数量和周期计算基础优先级
            workload_count = len(job.workloads)
            cycle = job.cycle
            
            # 较小的周期和较少的工作负载获得更高优先级
            base_priority = 1.0 / (workload_count * cycle + 1)
            
            # 考虑启动时间，越早启动的任务优先级越高
            start_time_factor = 1.0 / (self.schedules[job_id].start_time + 1)
            
            # 计算总优先级
            self.job_priorities[job_id] = base_priority * start_time_factor
    
    def get_link_balance_factor(self, link_id: int) -> float:
        """计算链路的负载均衡因子，值越低表示链路越空闲或负载越均衡"""
        if link_id not in self.link_utilization:
            return 0.0
        
        # 当前利用率
        current_util = self.link_utilization[link_id]
        
        # 如果有历史数据，计算方差
        if link_id in self.link_history and len(self.link_history[link_id]) > 1:
            variance = np.var(self.link_history[link_id])
            # 较高的方差表示链路负载波动大，我们希望避免使用这样的链路
            return current_util + variance * 0.5
        
        return current_util
    
    def get_bottleneck_links(self, threshold: float = 0.8) -> List[int]:
        """识别网络中的瓶颈链路"""
        bottlenecks = []
        for link_id, util in self.link_utilization.items():
            if util > threshold:
                bottlenecks.append(link_id)
        return bottlenecks
        
    def schedule(self) -> tuple[float, float]:
        """改进的贪心调度算法，考虑任务优先级和链路负载均衡"""
        total_flow = 0.0
        total_workload_bw = 0.0
        
        # 初始化数据结构
        self.link_traffic = {}
        self.change_points = {}
        self.link_peak_bw = {}
        self.link_utilization = {}
        
        # 初始化所有链路的容量
        for src_node, links in self.network.edges.items():
            for link in links:
                link_id = link.link_id
                # 直接将剩余带宽设置为链路容量（初始状态下全部可用）
                self.link_peak_bw[link_id] = link.capacity
                self.link_utilization[link_id] = 0.0
                self.link_traffic[link_id] = []
                self.change_points[link_id] = set()
                self.link_history[link_id] = []
        
        # 更新任务优先级
        self.update_job_priorities()
        
        # 创建工作负载列表，包含所有需要调度的工作负载
        all_workloads = []
        for job_id, job in self.jobs.items():
            priority = self.job_priorities.get(job_id, 0.0)
            for workload_id, workload in enumerate(job.workloads):
                # 计算每个工作负载的紧急度分数
                time_span = max(1, workload.t_e - workload.t_s)  # 确保不为0
                urgency = priority * (1.0 / time_span)
                all_workloads.append((job_id, workload_id, urgency, workload.bw))
        
        # 按紧急度排序工作负载
        all_workloads.sort(key=lambda x: x[2], reverse=True)
        
        # 第一轮：为所有工作负载分配最低保证带宽
        min_guarantee_ratio = 0.5
        for job_id, workload_id, urgency, demand_bw in all_workloads:
            workload = self.jobs[job_id].workloads[workload_id]
            tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
            
            # 计算瓶颈带宽
            bottleneck_bw = float("inf")
            for link in tunnel:
                link_id = link.link_id
                if link_id in self.link_peak_bw:
                    bottleneck_bw = min(bottleneck_bw, self.link_peak_bw[link_id])
                else:
                    # 如果链路不在记录中，使用链路的容量
                    self.link_peak_bw[link_id] = link.capacity
                    bottleneck_bw = min(bottleneck_bw, link.capacity)
            
            # 计算最低保证带宽 (确保至少分配一些带宽)
            min_guarantee = min(workload.bw * min_guarantee_ratio, bottleneck_bw)
            min_guarantee = max(min_guarantee, min(1.0, bottleneck_bw))  # 至少保证1Gbps或全部瓶颈带宽
            
            if min_guarantee > 0:
                # 更新所有受影响链路的可用带宽
                for link in tunnel:
                    link_id = link.link_id
                    if link_id in self.link_peak_bw:
                        self.link_peak_bw[link_id] -= min_guarantee
                
                # 只记录一次流量更新
                self.update_traffic_pattern(job_id, workload_id, min_guarantee)
                total_flow += min_guarantee
                
                # print(f"分配初始带宽: job_id={job_id}, workload_id={workload_id}, bw={min_guarantee:.2f}, bottleneck={bottleneck_bw:.2f}")
                
        # 更新所有链路的峰值带宽
        for link_id in range(self.network.link_num):
            self.calculate_peak_bw(link_id)
        
        # 第二轮：分配剩余带宽，优先考虑高紧急度的工作负载
        remaining_demands = []
        for job_id, workload_id, urgency, demand_bw in all_workloads:
            workload = self.jobs[job_id].workloads[workload_id]
            
            # 计算已分配的带宽
            allocated = 0
            # 查找此工作负载的已分配带宽
            for link_id, traffic_list in self.link_traffic.items():
                for traffic in traffic_list:
                    if (traffic.job_id == job_id and 
                        traffic.t_s == workload.t_s and 
                        traffic.t_e == workload.t_e):
                        allocated = traffic.bw
                        break
                if allocated > 0:
                    break
            
            # 计算剩余需求
            remaining = max(0, workload.bw - allocated)
            if remaining > 0:
                remaining_demands.append((job_id, workload_id, urgency, remaining))
                # print(f"需要分配剩余带宽: job_id={job_id}, workload_id={workload_id}, already={allocated:.2f}, remaining={remaining:.2f}")
        
        # 按紧急度排序剩余需求
        remaining_demands.sort(key=lambda x: x[2], reverse=True)
        
        # 分配剩余带宽
        for job_id, workload_id, urgency, remaining in remaining_demands:
            workload = self.jobs[job_id].workloads[workload_id]
            tunnel: Tunnel = self.schedules[job_id].tunnels[workload_id]
            
            # 计算当前瓶颈带宽
            bottleneck_bw = float("inf")
            for link in tunnel:
                link_id = link.link_id
                if link_id in self.link_peak_bw:
                    bottleneck_bw = min(bottleneck_bw, self.link_peak_bw[link_id])
                else:
                    bottleneck_bw = 0  # 如果链路不存在，则无可用带宽
                    break
            
            # 确定分配带宽
            additional_bw = min(remaining, bottleneck_bw)
            
            if additional_bw > 0:
                # print(f"分配剩余带宽: job_id={job_id}, workload_id={workload_id}, additional={additional_bw:.2f}, bottleneck={bottleneck_bw:.2f}")
                
                # 更新所有受影响链路的可用带宽
                for link in tunnel:
                    link_id = link.link_id
                    if link_id in self.link_peak_bw:
                        self.link_peak_bw[link_id] -= additional_bw
                
                # 查找此工作负载的现有流量记录
                existing_traffic = None
                for link_id, traffic_list in self.link_traffic.items():
                    for traffic in traffic_list:
                        if (traffic.job_id == job_id and 
                            traffic.t_s == workload.t_s and 
                            traffic.t_e == workload.t_e):
                            # 找到现有流量记录
                            existing_traffic = traffic
                            break
                    if existing_traffic:
                        break
                
                if existing_traffic:
                    # 更新现有流量
                    existing_traffic.bw += additional_bw
                    total_flow += additional_bw
                else:
                    # 创建新的流量记录
                    self.update_traffic_pattern(job_id, workload_id, additional_bw)
                    total_flow += additional_bw
        
        # 计算总工作负载带宽需求
        for job_id, job in self.jobs.items():
            for workload in job.workloads:
                total_workload_bw += workload.bw

        return total_flow, total_workload_bw