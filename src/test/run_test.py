import json
import os
import pandas as pd
from network.graph import Graph
from phase1.admission_control import AdmissionController
from job.job_info import JobInfo, EPOCH
from job.workload import Workload

def run_admission_control(topology_file: str, jobs_file: str) -> None:

    # 加载拓扑
    topology_df = pd.read_csv(topology_file)
    # 加载任务
    with open(jobs_file, 'r') as f:
        jobs_data = json.load(f)
    # 生成测例结果目录
    testcase_name = os.path.splitext(os.path.basename(jobs_file))[0]
    result_dir = os.path.join("result", testcase_name)
    os.makedirs(result_dir, exist_ok=True)

    # 输入：网络 + 任务
    # 网络拓扑
    network: Graph = Graph.from_dataframe(topology_df)
    # 任务信息
    jobs: list[JobInfo] = []
    for job in jobs_data:
        job_id = job['job_id']
        cycle = (job['cycle(ms)'] + EPOCH - 1) // EPOCH # 向上取整
        workloads = []
        for workload in job['workloads']:
            src = workload['src_rank']
            dst = workload['dst_rank']
            t_s = workload['start_timestamp(ms)'] // EPOCH # 向下取整
            t_e = (workload['end_timestamp(ms)'] + EPOCH - 1) // EPOCH # 向上取整
            bw = workload['bandwidth(Gbps)']
            workload = Workload(src, dst, t_s, t_e, bw)
            workloads.append(workload)
        job_info = JobInfo(job_id, cycle, workloads)
        jobs.append(job_info)

    # 输出：
    # a_j = {0, 1}，表示任务 j 是否准入
    a = [0] * len(jobs_data)

    admission_controller = AdmissionController(network)
    for job_id, job in enumerate(jobs):

        # Step 1：直接部署
        a[job_id] = admission_controller.direct_deploy(job)

        # Step 2: 局部调整
        if a[job_id] == 0:
            a[job_id] = admission_controller.local_adjust(job)

    print("Total admitted jobs:", sum(a))

if __name__ == '__main__':
    
    topology_file = 'multi_job/link_list_tmp.csv'
    
    for i in range(1, 51):
        jobs_file = f'testcases/testcase{i}.json'
        if os.path.exists(jobs_file):
            print(f"\ntestcase {i}")
            try:
                run_admission_control(topology_file, jobs_file)
            except Exception as e:
                print(f"Error processing testcase {i}: {str(e)}")
        else:
            print(f"Testcase {i} not found: {jobs_file}")

