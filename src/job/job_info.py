from dataclasses import dataclass
from workload import Workload

EPOCH = 10 # 1 epoch = () ms

@dataclass
class JobInfo:
    job_id: int
    cycle: int # (epoch)
    workloads: list[Workload]