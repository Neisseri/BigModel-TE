from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule, Traffic, Tunnel
from gurobipy import Model, GRB
from params import SCHEDULE_INTERVAL

# TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
overlap_circle = SCHEDULE_INTERVAL

class TrafficScheduler:
    def __init__(self, network: Graph, jobs: dict[int, JobInfo], schedules: dict[int, JobSchedule]):
        self.network = network
        self.jobs = jobs
        self.schedules = schedules

        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 链路所有流量变化的时间点
        self.change_points: dict[int, set[int]] = {} # link_id -> set[int]
        # 链路峰值带宽
        self.link_peak_bw: dict[int, float] = {} # link_id -> peak_bandwidth

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