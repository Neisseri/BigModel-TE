from job.job_info import JobInfo, EPOCH
from job.workload import Workload
import random

def random_fluctuate(jobs: list[JobInfo]) -> list[JobInfo]:
    new_jobs: list[JobInfo] = []
    for job in jobs:
        new_job = JobInfo(job.job_id, job.cycle, [])
        new_jobs.append(new_job)
        for workload in job.workloads:
            new_workload = Workload(
                src=workload.src,
                dst=workload.dst,
                t_s=workload.t_s,
                t_e=workload.t_e,
                bw=workload.bw
            )
            new_job.workloads.append(new_workload)
            
    for job_id, job in enumerate(new_jobs):
        for workload in job.workloads:
            # 以一定概率对 workload 进行修改
            if random.uniform(0, 1) < 0.7 and job_id != 0:
                continue

            # 随机扰动开始时间和结束时间，偏移范围为 [-2 * EPOCH, 2 * EPOCH]
            time_offset = random.randint(-2 * EPOCH, 2 * EPOCH)
            workload.t_s = max(0, min(job.cycle, workload.t_s + time_offset))
            workload.t_e = max(workload.t_s, min(job.cycle, workload.t_e + time_offset))

            # 随机扰动带宽，变化范围为 ±10%
            bandwidth_offset = workload.bw * random.uniform(-0.1, 0.1)
            workload.bw = max(0, workload.bw + bandwidth_offset)
    
    return new_jobs