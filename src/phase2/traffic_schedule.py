from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule, Traffic, Tunnel
from gurobipy import Model, GRB
from params import SCHEDULE_INTERVAL

class TrafficScheduler:
    def __init__(self, network: Graph, old_jobs: list[JobInfo], old_schedules: dict[int, JobSchedule]):
        self.network = network
        self.old_jobs = old_jobs
        self.old_schedules = old_schedules

        self.new_jobs: list[JobInfo] = []

        self.updated_workloads: list[tuple[int, int]] = [] # [i, j]: 第 i 个任务的第 j 个workload

        self.link_traffic: dict[int, list[Traffic]] = {} # 链路上经过的流量模式 link_id -> list[Traffic]
        # 链路所有流量变化的时间点
        self.change_points: dict[int, set[int]] = {} # link_id -> set[int]
        # 链路峰值带宽
        self.link_peak_bw: dict[int, float] = {} # link_id -> peak_bandwidth
        # 链路峰值带宽所在时间点
        self.link_peak_bw_points: dict[int, int] = {} # link_id -> peak_bandwidth_time_point

    def update_peak_bw(self, link_id: int):
        # TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
        overlap_circle = SCHEDULE_INTERVAL

        if link_id not in self.link_traffic:
            self.link_traffic[link_id] = []
            self.change_points[link_id] = set()
        for traffic in self.link_traffic[link_id]:
            # 添加新流量的变化时间点
            for circle_offset in range(0, overlap_circle, traffic.cycle):
                start = (traffic.t_s + circle_offset + self.old_schedules[traffic.job_id].start_time) % overlap_circle
                end = (traffic.t_e + circle_offset + self.old_schedules[traffic.job_id].start_time) % overlap_circle
                            
                self.change_points[link_id].add(start)
                self.change_points[link_id].add(end)

        # 更新链路峰值带宽
        peak_bw = 0.0
        # 在每个流量变化时间点计算总带宽
        for time in sorted(list(self.change_points[link_id])):
            bw_now = 0.0
            for traffic in self.link_traffic[link_id]:
                time_in_circle = (time + traffic.cycle - self.old_schedules[traffic.job_id].start_time) % traffic.cycle
                if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                    bw_now += traffic.bw
            if bw_now >= peak_bw:
                peak_bw = bw_now
                self.link_peak_bw_points[link_id] = time
        self.link_peak_bw[link_id] = peak_bw

    def update_traffic_pattern(self):

        for job in self.old_jobs:
            job_id = job.job_id
            if self.old_schedules[job_id].admit == 0:
                continue
            for workload_id, workload in enumerate(job.workloads):
                if (job_id, workload_id) in self.updated_workloads:
                    continue
                else:
                    tunnel: Tunnel = self.old_schedules[job_id].tunnels[workload_id]
                    for link in tunnel:
                        if link.link_id not in self.link_traffic:
                            self.link_traffic[link.link_id] = []
                            self.change_points[link.link_id] = set()
                        self.link_traffic[link.link_id].append(
                            Traffic(
                                job_id=job.job_id,
                                cycle=job.cycle,
                                t_s=workload.t_s,
                                t_e=workload.t_e,
                                bw=workload.bw
                            )
                        )

        # for link_id in range(self.network.link_num):
        #     self.update_peak_bw(link_id)

    def calculate_bottleneck_bw(self, tunnel: Tunnel, job_id: int, workload_id: int) -> float:
        
        bottleneck_bw = float("inf")

        for link in tunnel:
            # TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
            cycle = self.new_jobs[job_id].cycle
            t_s = (self.new_jobs[job_id].workloads[workload_id].t_s + self.old_schedules[job_id].start_time) % cycle
            t_e = (self.new_jobs[job_id].workloads[workload_id].t_e + self.old_schedules[job_id].start_time) % cycle
            
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
                        time_in_circle = (time + traffic.cycle - self.old_schedules[traffic_job_id].start_time) % traffic.cycle
                        if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                            bw_now += traffic.bw
                    if bw_now >= link_alloc_bw:
                        link_alloc_bw = bw_now

            if link.capacity - link_alloc_bw < bottleneck_bw:
                bottleneck_bw = link.capacity - link_alloc_bw
        return bottleneck_bw

    def update_schedule(self, new_jobs: list[JobInfo]) -> tuple[dict[int, JobSchedule], float]:

        # TODO: 传入任务顺序是打乱的，所以枚举的job_id和实际job_id可能不一致，需要从头过一遍

        self.new_jobs = new_jobs

        # 创建 Gurobi 模型
        model = Model("TrafficScheduler")
        model.setParam('OutputFlag', 0)  # 关闭输出日志

        workload_num = 0
        for new_job in new_jobs:
            new_job_id = new_job.job_id
            if self.old_schedules[new_job_id].admit == 0:
                continue
            old_job = self.old_jobs[new_job_id]
            for workload_id, new_workload in enumerate(new_job.workloads):
                old_workload = old_job.workloads[workload_id]
                # print(f"old_bw = {old_workload.bw}, new_bw = {new_workload.bw}")
                if (new_workload.t_e != old_workload.t_e
                    or new_workload.t_s != old_workload.t_s
                    or new_workload.bw != old_workload.bw):
                    # 负载预测信息更新
                    self.updated_workloads.append((new_job_id, workload_id))
                    workload_num += 1

        self.update_traffic_pattern()
                
        # 添加变量：updated_workload 中每个负载分配的流量大小
        # TODO: 此时每个负载分配一条流，后续需要修改为多隧道
        flow_vars = {}
        for job_id, workload_id in self.updated_workloads:
            flow_vars[(job_id, workload_id)] = model.addVar(
                vtype=GRB.CONTINUOUS,
                name=f"flow_{job_id}_{workload_id}",
                lb=0.0,
                ub=float("inf")
            )

        # 设置目标函数：最大化总流量
        model.setObjective(
            sum(flow_vars[(job_id, workload_id)] for job_id, workload_id in self.updated_workloads),
            GRB.MAXIMIZE
        )

        # 链路容量约束
        # TODO: 为了简化，不考虑更新流之间的重叠（即每个约束只有一个变量）
        # 可以通过减小数据集中更新的负载数来降低这个简化的负面效果，后续再修改
        for job_id, workload_id in self.updated_workloads:
            tunnel: Tunnel = self.old_schedules[job_id].tunnels[workload_id]

            # 计算瓶颈带宽
            bottleneck_bw = self.calculate_bottleneck_bw(tunnel, job_id, workload_id)
            print("bottleneck_bw = ", bottleneck_bw)
            # TODO: 没有找出为什么瓶颈带宽会为负，这是一个需要修复的bug
            # if bottleneck_bw < 0:
            #     bottleneck_bw = 0.0
            # print(f"bottleneck_bw = {bottleneck_bw}")
            model.addConstr(
                flow_vars[(job_id, workload_id)] <= bottleneck_bw,
                name=f"link_capacity_{job_id}_{workload_id}"
            )

        # 带宽需求约束
        for job_id, workload_id in self.updated_workloads:
            workload: Workload = new_jobs[job_id].workloads[workload_id]
            model.addConstr(
                flow_vars[(job_id, workload_id)] <= workload.bw,
                name=f"bw_demand_{job_id}_{workload_id}"
            )

        # 求解模型
        model.optimize()

        print("status =", model.status)

        # 检查是否找到可行解
        if model.status in [GRB.OPTIMAL, GRB.SUBOPTIMAL, GRB.TIME_LIMIT, GRB.SOLUTION_LIMIT]:
            # 复制调度结果实例
            new_schedules: dict[int, JobSchedule] = {}
            for job_id, old_schedule in self.old_schedules.items():
                new_schedules[job_id] = JobSchedule(
                    admit=old_schedule.admit,
                    start_time=old_schedule.start_time,
                    tunnels=[],
                    bw_alloc=[]
                )
                for tunnel in old_schedule.tunnels:
                    new_schedules[job_id].tunnels.append(tunnel)
                for bw in old_schedule.bw_alloc:
                    new_schedules[job_id].bw_alloc.append(bw)

            total_flow = 0.0
            for job_id, workload_id in self.updated_workloads:
                new_schedules[job_id].bw_alloc[workload_id] = flow_vars[(job_id, workload_id)].X
                total_flow += new_schedules[job_id].bw_alloc[workload_id]
            print(f"TE total flow = {total_flow}")
            return new_schedules, total_flow
        else:
            raise ValueError("Gurobi failed to find a feasible solution.")
        
    def greedy_alloc(self) -> float:
        total_flow = 0.0
        for job_id, workload_id in self.updated_workloads:
            workload: Workload = self.old_jobs[job_id].workloads[workload_id]
            tunnel: Tunnel = self.old_schedules[job_id].tunnels[workload_id]
            bottleneck_bw = float("inf")
            for link in tunnel:
                bottleneck_bw = min(bottleneck_bw, link.capacity - self.link_peak_bw[link.link_id])
            # TODO: 没有找出为什么瓶颈带宽会为负，这是一个需要修复的bug
            # if bottleneck_bw < 0:
            #     bottleneck_bw = 0.0
            # 直接分配到链路剩余带宽
            workload.bw = min(workload.bw, bottleneck_bw)
            # 更新链路流量
            traffic = Traffic(
                job_id=job_id,
                cycle=self.old_jobs[job_id].cycle,
                t_s=workload.t_s,
                t_e=workload.t_e,
                bw=workload.bw
            )
            for link in tunnel:
                link_id = link.link_id
                if link_id not in self.link_traffic:
                    self.link_traffic[link_id] = []
                    self.change_points[link_id] = set()
                # 添加流量
                self.link_traffic[link_id].append(traffic)
            # 更新链路峰值带宽
            self.update_peak_bw(link_id)
            total_flow += workload.bw
        print("Greedy total flow: ", total_flow)
        return total_flow