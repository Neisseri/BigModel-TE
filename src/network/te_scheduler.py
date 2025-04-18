import gurobipy as gp
from gurobipy import GRB
from dataclasses import dataclass
from .demand import Demand
from .graph import Graph, Link
from .path_finder import PathFinder
import numpy as np
from typing import Optional
import copy
from .scheduler_base import SchedulerBase, ScheduleResult, PathBwAlloc, SDPair
from typing import List, Dict, Tuple, Set
from collections import defaultdict

class TEScheduler(SchedulerBase):

    def __init__(self, graph: Graph, path_finder: PathFinder):
        super().__init__(graph, path_finder)
        
    def schedule_job(self, job: dict):
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

        result: ScheduleResult = self._solve_lp(job['job_id'], demands, job['cycle(ms)'])
        if result.success:
            self.results.append(result)

    def _solve_lp(self, job_id: int, demands: list[Demand], cycle_len: int) -> ScheduleResult:
        # 创建环境并设置参数
        env = gp.Env(empty=True)
        env.setParam('OutputFlag', 0)
        env.start()
        
        model = gp.Model("Baseline_TE", env=env)
        
        # 获取所有可能的路径
        paths_dict = {}  # (src, dst) -> list[paths]
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

        # 优化目标：最小化总带宽使用
        # TODO: 改成最小化 MLU
        model.setObjective(
            gp.quicksum(flow for flows in link_flows.values() 
                       for flow in flows),
            GRB.MINIMIZE
        )

        model.optimize()

        if model.status == GRB.OPTIMAL:
            # 构造路径分配结果
            paths_allocation: dict[int, list[list[SDPair], float]] = {}
            total_allocated_bw = 0.0
            link_bw_usage = defaultdict(float)
            
            for demand_idx, demand in enumerate(demands):
                paths = paths_dict[(demand.src_rank, demand.dst_rank)]
                path_allocs = []  # [(path, bandwidth)]
                
                for path_idx, path in enumerate(paths):
                    flow = flow_vars[(demand_idx, path_idx)].X
                    if flow > 1e-6:
                        # 将Link转换为SDPair列表
                        sd_path = [(link.src_rank, link.dst_rank) for link in path]
                        path_allocs.append([sd_path, flow])
                        
                        total_allocated_bw += flow
                        for link in path:
                            link_key = (link.src_rank, link.dst_rank)
                            link_bw_usage[link_key] += flow
                
                if path_allocs:
                    paths_allocation[demand_idx] = path_allocs

            # 更新调度器状态
            self.link_peak_bandwidth = dict(link_bw_usage)
            self.all_links.update(link_bw_usage.keys())

            # 更新流量模式
            self.update_link_traffic_pattern(job_id, cycle_len, demands, paths_allocation)

            return ScheduleResult(
                job_id=job_id,
                success=True,
                start_time=0,
                paths_allocation=paths_allocation
            )
        else:
            return ScheduleResult(
                job_id=job_id,
                success=False,
                start_time=0,
                paths_allocation={}
            )
