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
                job_id = traffic.job_id
                time_in_circle = (time + traffic.cycle - self.old_schedules[job_id].start_time) % traffic.cycle
                if time_in_circle >= traffic.t_s and time_in_circle < traffic.t_e:
                    bw_now += traffic.bw
            if bw_now >= peak_bw:
                peak_bw = bw_now
                self.link_peak_bw_points[link_id] = time
        self.link_peak_bw[link_id] = peak_bw

    def update_traffic_pattern(self, updated_workloads: list[tuple[int, int]]):

        for job_idx, job in enumerate(self.old_jobs):
            for workload_idx, workload in enumerate(job.workloads):
                if (job_idx, workload_idx) in updated_workloads:
                    continue
                else:
                    tunnel: Tunnel = self.old_schedules[job_idx].tunnels[workload_idx]
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

        for link_id in range(self.network.link_num):
            self.update_peak_bw(link_id)

    def update_schedule(self, new_jobs: list[JobInfo]) -> dict[int, JobSchedule]:

        # 创建 Gurobi 模型
        model = Model("TrafficScheduler")
        model.setParam('OutputFlag', 0)  # 关闭输出日志

        updated_workloads: list[tuple[int, int]] = [] # [i, j]: 第 i 个任务的第 j 个workload
        workload_num = 0
        for job_idx, new_job in enumerate(new_jobs):
            old_job = self.old_jobs[job_idx]
            for workload_idx, new_workload in enumerate(new_job.workloads):
                old_workload = old_job.workloads[workload_idx]
                # print(f"old_bw = {old_workload.bw}, new_bw = {new_workload.bw}")
                if (new_workload.t_e != old_workload.t_e
                    or new_workload.t_s != old_workload.t_s
                    or new_workload.bw != old_workload.bw):
                    # 负载预测信息更新
                    updated_workloads.append((job_idx, workload_idx))
                    workload_num += 1

        print(f"Updated workloads num = {workload_num}")

        self.update_traffic_pattern(updated_workloads)
                
        # 添加变量：updated_workload 中每个负载分配的流量大小
        # TODO: 此时每个负载分配一条流，后续需要修改为多隧道
        flow_vars = {}
        for updated_workload in updated_workloads:
            flow_vars[(updated_workload[0], updated_workload[1])] = model.addVar(
                vtype=GRB.CONTINUOUS,
                name=f"flow_{updated_workload[0]}_{updated_workload[1]}"
            )

        # 设置目标函数：最大化总流量
        model.setObjective(
            sum(flow_vars[(job_idx, workload_idx)] for job_idx, workload_idx in updated_workloads),
            GRB.MAXIMIZE
        )

        # 链路容量约束
        # TODO: 为了简化，不考虑更新流之间的重叠（即每个约束只有一个变量）
        # 可以通过减小数据集中更新的负载数来降低这个简化的负面效果，后续再修改
        for job_idx, workload_idx in updated_workloads:
            tunnel: Tunnel = self.old_schedules[job_idx].tunnels[workload_idx]
            bottleneck_bw = float("inf")
            for link in tunnel:
                bottleneck_bw = min(bottleneck_bw, link.capacity - self.link_peak_bw[link.link_id])
            model.addConstr(
                flow_vars[(job_idx, workload_idx)] <= bottleneck_bw,
                name=f"link_capacity_{job_idx}_{workload_idx}"
            )

        # 带宽需求约束
        for job_idx, workload_idx in updated_workloads:
            workload: Workload = new_jobs[job_idx].workloads[workload_idx]
            model.addConstr(
                flow_vars[(job_idx, workload_idx)] <= workload.bw,
                name=f"bw_demand_{job_idx}_{workload_idx}"
            )

        # 求解模型
        model.optimize()

        # 解析结果
        if model.status == GRB.OPTIMAL:
            # 复制调度结果实例
            new_schedules: dict[int, JobSchedule] = {}
            for job_idx, old_schedule in self.old_schedules.items():
                new_schedules[job_idx] = JobSchedule(
                    admit=old_schedule.admit,
                    start_time=old_schedule.start_time,
                    tunnels=[],
                    bw_alloc=[]
                )
                for tunnel in old_schedule.tunnels:
                    new_schedules[job_idx].tunnels.append(tunnel)
                for bw in old_schedule.bw_alloc:
                    new_schedules[job_idx].bw_alloc.append(bw)

            total_flow = 0.0
            for job_idx, workload_idx in updated_workloads:
                y = new_schedules[job_idx].bw_alloc[workload_idx]
                new_schedules[job_idx].bw_alloc[workload_idx] = flow_vars[(job_idx, workload_idx)].X
                total_flow += new_schedules[job_idx].bw_alloc[workload_idx]
                print(f"Job {job_idx}, Workload {workload_idx}: {new_schedules[job_idx].bw_alloc[workload_idx]}")
            print(f"Total flow: {total_flow}")
            return new_schedules
        else:
            raise ValueError("Gurobi failed to find an optimal solution.")