from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule, Traffic, Tunnel
from gurobipy import Model, GRB
from params import SCHEDULE_INTERVAL

# TODO: 这里为了方便直接设置成 SCHEDULE_INTERVAL，因为算出来的最小公倍数可能远远大于这个数。具体如何处理后续再考虑
overlap_circle = SCHEDULE_INTERVAL

class Greedy:
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
    
    def calculate_peak_bw(self, link_id: int):
        
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
        
    def greedy_alloc(self) -> tuple[float, float]:

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
                    bottleneck_bw = min(bottleneck_bw, link.capacity - self.link_peak_bw[link.link_id])
                alloc_bw = min(workload.bw, bottleneck_bw)
                # print("job_id: ", job_id, " workload_id: ", workload_id, " alloc_bw: ", alloc_bw)
                
                for link in tunnel:
                    self.update_traffic_pattern(job_id, workload_id, alloc_bw)
                    self.link_peak_bw[link.link_id] += alloc_bw
                    
                total_flow += alloc_bw
                total_workload_bw += workload.bw
            
            for link_id in update_link_id:
                self.calculate_peak_bw(link_id)

        return total_flow, total_workload_bw