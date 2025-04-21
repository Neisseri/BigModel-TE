from network.graph import Graph
from job.job_info import JobInfo
from job.workload import Workload
from phase1.admission_control import JobSchedule

SCHEDULE_INTERVAL = 1000 # (epoch)

class TrafficScheduler:
    def __init__(self, network: Graph, old_jobs: list[JobInfo], old_schedules: dict[int, JobSchedule]):

        self.network = network
        self.old_jobs = old_jobs
        self.old_schedules = old_schedules

    def update_schedule(self, new_jobs: list[JobInfo]) -> dict[int, JobSchedule]:
        return self.old_schedules