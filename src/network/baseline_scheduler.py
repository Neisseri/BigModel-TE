import gurobipy as gp
from gurobipy import GRB
from collections import defaultdict
from .scheduler_base import SchedulerBase, ScheduleResult
from .demand import Demand
from typing import List, Dict, Tuple, Set
import numpy as np

class BaselineScheduler(SchedulerBase):
    def __init__(self, graph, path_finder):
        super().__init__(graph, path_finder)
        self.time_slots = 100  # 时间离散化的槽数
        self.results = []  # 存储所有任务的调度结果

    def schedule_job(self, job: Dict) -> ScheduleResult:
        demands = []
        for demand_data in job['demands']:
            demand = Demand(
                job_id=job['job_id'],
                src_rank=demand_data['src_rank'],
                dst_rank=demand_data['dst_rank'],
                start_time=demand_data['start_timestamp(ms)'],
                end_time=demand_data['end_timestamp(ms)'],
                bandwidth=demand_data['bandwidth(Gbps)']
            )
            demands.append(demand)

        result = self._solve_lp(job['job_id'], demands, job['cycle(ms)'])
        if result.success:
            result.job_id = job['job_id']
            self.results.append(result)
        return result

    def _solve_lp(self, job_id: int, demands: List[Demand], cycle_len: int) -> ScheduleResult:
        # 创建环境并设置参数
        env = gp.Env(empty=True)
        env.setParam('OutputFlag', 0)
        env.start()
        
        model = gp.Model("Baseline_TE", env=env)
        
        # 获取所有可能的路径
        paths_dict = {}  # (src, dst) -> List[paths]
        for demand in demands:
            key = (demand.src_rank, demand.dst_rank)
            if key not in paths_dict:
                paths_dict[key] = self.path_finder.find_all_paths(*key)

        # 创建变量
        flow_vars = {}  # (demand_idx, path_idx) -> var
        for demand_idx, demand in enumerate(demands):
            paths = paths_dict[(demand.src_rank, demand.dst_rank)]
            for path_idx in range(len(paths)):
                flow_vars[(demand_idx, path_idx)] = model.addVar(
                    name=f"flow_{demand_idx}_{path_idx}",
                    lb=0,
                    ub=demand.bandwidth
                )

        # 添加需求满足约束
        for demand_idx, demand in enumerate(demands):
            paths = paths_dict[(demand.src_rank, demand.dst_rank)]
            model.addConstr(
                gp.quicksum(flow_vars[(demand_idx, i)] for i in range(len(paths))) 
                == demand.bandwidth,
                name=f"demand_{demand_idx}"
            )

        # 添加链路容量约束
        link_flows = {}  # (src, dst) -> List[flow vars]
        for demand_idx, demand in enumerate(demands):
            paths = paths_dict[(demand.src_rank, demand.dst_rank)]
            for path_idx, path in enumerate(paths):
                for link in path:
                    link_key = (link.src_rank, link.dst_rank)
                    if link_key not in link_flows:
                        link_flows[link_key] = []
                    link_flows[link_key].append(flow_vars[(demand_idx, path_idx)])

        for (src, dst), flows in link_flows.items():
            link = self.graph.get_link(src, dst)
            if link:
                model.addConstr(
                    gp.quicksum(flows) <= link.bandwidth,
                    name=f"capacity_{src}_{dst}"
                )

        # 设置目标：最小化总带宽使用
        model.setObjective(
            gp.quicksum(flow for flows in link_flows.values() 
                       for flow in flows),
            GRB.MINIMIZE
        )

        # 求解
        model.optimize()

        if model.status == GRB.OPTIMAL:
            # 构造结果
            paths_allocation = {}
            total_allocated_bw = 0.0
            link_bw_usage = defaultdict(float)
            
            for demand_idx, demand in enumerate(demands):
                paths = paths_dict[(demand.src_rank, demand.dst_rank)]
                allocations = []
                for path_idx, path in enumerate(paths):
                    flow = flow_vars[(demand_idx, path_idx)].X
                    if flow > 1e-6:
                        allocations.append((path, flow))
                        total_allocated_bw += flow
                        for link in path:
                            link_key = (link.src_rank, link.dst_rank)
                            link_bw_usage[link_key] += flow
                paths_allocation[demand_idx] = allocations

            # 更新类成员变量
            self.link_peak_bandwidth = link_bw_usage
            self.all_links.update(link_bw_usage.keys())

            return ScheduleResult(
                True, 0, paths_allocation, total_allocated_bw,
                sum(link_bw_usage.values())
            )
        else:
            return ScheduleResult(
                False, 0, {}, 0.0, 0.0,
                {"reason": f"LP solving failed with status: {model.status}"}
            )
