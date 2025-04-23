from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule
from gurobipy import Model, GRB
from params import SCHEDULE_INTERVAL

class TrafficScheduler:
    def __init__(self, network: Graph, old_jobs: list[JobInfo], old_schedules: dict[int, JobSchedule]):

        self.network = network
        self.old_jobs = old_jobs
        self.old_schedules = old_schedules

    def update_schedule(self, new_jobs: list[JobInfo]) -> dict[int, JobSchedule]:
        # 初始化变量
        all_workloads = []
        for job in new_jobs:
            all_workloads.extend(job.workloads)

        num_links = self.network.link_num
        num_workloads = len(all_workloads)

        # 创建 Gurobi 模型
        model = Model("TrafficScheduler")
        model.setParam('OutputFlag', 0)  # 关闭输出日志

        # 添加变量：每个 workload 在每条链路上的流量分配
        # f_q^t 表示 workload q 在链路 t 上的流量
        flow_vars = {}
        for q, workload in enumerate(all_workloads):
            for link_id in range(1, num_links + 1):
                flow_vars[(q, link_id)] = model.addVar(lb=0, name=f"f_{q}_{link_id}")

        # 设置目标函数：最大化总流量
        model.setObjective(
            sum(flow_vars[(q, link_id)] for q in range(num_workloads) for link_id in range(1, num_links + 1)),
            GRB.MAXIMIZE
        )

        # 添加约束 (2): 链路容量约束
        for link_id in range(1, num_links + 1):
            for r in range(SCHEDULE_INTERVAL):
                model.addConstr(
                    sum(flow_vars[(q, link_id)] for q, workload in enumerate(all_workloads)
                        if link_id in [link.link_id for link in self.network.edges.get(workload.src, [])]) <=
                    self.network.edges[link_id][0].capacity,
                    name=f"capacity_{link_id}_{r}"
                )

        # 添加约束 (3): 带宽需求约束
        for q, workload in enumerate(all_workloads):
            model.addConstr(
                sum(flow_vars[(q, link_id)] for link_id in range(1, num_links + 1)) <= workload.bandwidth,
                name=f"bandwidth_{q}"
            )

        # 求解模型
        model.optimize()

        # 解析结果
        if model.status == GRB.OPTIMAL:
            new_schedules = {}
            for q, workload in enumerate(all_workloads):
                schedule = {}
                for link_id in range(1, num_links + 1):
                    flow = flow_vars[(q, link_id)].x
                    if flow > 0:
                        schedule[link_id] = flow
                new_schedules[workload] = schedule
            return new_schedules
        else:
            raise ValueError("Gurobi failed to find an optimal solution.")