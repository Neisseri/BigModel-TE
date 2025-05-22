import pandas as pd
import random
import json
import argparse
import os

# 随机参数范围
JOB_NUM = (1, 51) # 任务数
CYCLE = (200, 1000) # 迭代周期
WORKLOAD_NUM = (5, 10) # 负载数
BANDWIDTH = (50, 70) # 带宽需求

def get_host_nodes(topology_file: str) -> list[int]:
    # 读取所有 Host 节点
    df = pd.read_csv(topology_file)
    hosts = set()
    for _, row in df.iterrows():
        if row['a_node_type'] == 'HOST':
            hosts.add(row['a_node_id'])
        if row['z_node_type'] == 'HOST':
            hosts.add(row['z_node_id'])
    return sorted(list(hosts))

def generate_jobs(host_nodes: list[int], case_id: int) -> list[dict]:
    
    # 随机生成多个任务的 Workload
    jobs = []
    # job_num = random.randint(JOB_NUM[0], JOB_NUM[1])
    job_num = case_id
    
    for job_id in range(job_num):
        cycle = random.randint( CYCLE[0], CYCLE[1])
        workload_num = random.randint(WORKLOAD_NUM[0], WORKLOAD_NUM[1])
        workloads = []
        
        for _ in range(workload_num):
            # 源-目的节点
            src = random.choice(host_nodes)
            dst = random.choice([n for n in host_nodes if n != src])
            
            # 带宽需求
            bandwidth = random.randint(BANDWIDTH[0], BANDWIDTH[1])
            
            # 负载时间窗口
            start_time = random.randint(0, cycle - 100)
            end_time = random.randint(start_time + 50, min(start_time + 200, cycle))
            
            workloads.append({
                "src_rank": src,
                "dst_rank": dst,
                "start_timestamp(ms)": start_time,
                "end_timestamp(ms)": end_time,
                "bandwidth(Gbps)": bandwidth
            })
        
        jobs.append({
            "job_id": job_id,
            "cycle(ms)": cycle,
            "workloads": workloads
        })
    
    return jobs

def generate_batch_workloads(topology_file: str, output_dir: str, num_cases: int = 50) -> None:

    host_nodes = get_host_nodes(topology_file)
    
    # 创建负载文件输出目录，清空旧文件
    os.makedirs(output_dir, exist_ok=True)
    for filename in os.listdir(output_dir):
        file_path = os.path.join(output_dir, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")
    
    for case_id in range(1, num_cases + 1):
        jobs = generate_jobs(host_nodes, case_id)
        output_file = os.path.join(output_dir, f"testcase{case_id}.json")
        
        with open(output_file, 'w') as f:
            json.dump(jobs, f, indent=2)
        print(f"Generated testcase {case_id} saved to {output_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate random workload data')
    parser.add_argument('--topology', type=str, default='data/topology/link_list.csv',
                      help='Path to the topology CSV file')
    parser.add_argument('--output', type=str, default='data/jobs',
                      help='Directory to save the output jobs files')
    parser.add_argument('--num-cases', type=int, default=50,
                      help='Number of test cases to generate')
    parser.add_argument('--seed', type=int, default=None,
                      help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed)
    
    generate_batch_workloads(args.topology, args.output, args.num_cases)
