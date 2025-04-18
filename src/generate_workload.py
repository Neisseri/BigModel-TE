import pandas as pd
import random
import json
import argparse
import os

def get_host_nodes(topology_file: str) -> list[int]:
    """获取所有 HOST 类型的节点"""
    df = pd.read_csv(topology_file)
    hosts = set()
    for _, row in df.iterrows():
        if row['a_node_type'] == 'HOST':
            hosts.add(row['a_node_id'])
        if row['z_node_type'] == 'HOST':
            hosts.add(row['z_node_id'])
    return sorted(list(hosts))

def generate_random_workload(host_nodes: list[int]) -> list[dict]:
    """生成随机workload数据"""
    workload = []

    num_jobs = random.randint(10, 50)
    
    for job_id in range(num_jobs):
        # 随机生成一个200-3000ms范围内的周期
        cycle = random.randint(200, 3000)
        
        # 每个作业随机生成1-3个demands
        num_demands = random.randint(1, 5)
        demands = []
        
        for _ in range(num_demands):
            # 随机选择源和目的节点
            src_rank = random.choice(host_nodes)
            dst_rank = random.choice([n for n in host_nodes if n != src_rank])
            
            # 随机生成带宽需求 (5-100 Gbps)
            bandwidth = random.randint(5, 100)
            
            # 随机生成时间窗口，确保在周期内且start < end
            start_time = random.randint(0, cycle - 100)
            end_time = random.randint(start_time + 50, min(start_time + 200, cycle))
            
            demands.append({
                "src_rank": src_rank,
                "dst_rank": dst_rank,
                "start_timestamp(ms)": start_time,
                "end_timestamp(ms)": end_time,
                "bandwidth(Gbps)": bandwidth
            })
        
        workload.append({
            "job_id": job_id,
            "cycle(ms)": cycle,
            "demands": demands
        })
    
    return workload

def generate_batch_workloads(topology_file: str, output_dir: str, num_cases: int = 50) -> None:
    """生成多个测试用例"""
    host_nodes = get_host_nodes(topology_file)
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    for case_id in range(1, num_cases + 1):
        workload = generate_random_workload(host_nodes)
        output_file = os.path.join(output_dir, f"testcase{case_id}.json")
        
        with open(output_file, 'w') as f:
            json.dump(workload, f, indent=2)
        print(f"Generated testcase {case_id} saved to {output_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate random workload data')
    parser.add_argument('--topology', type=str, default='multi_job/link_list.csv',
                      help='Path to the topology CSV file')
    parser.add_argument('--output', type=str, default='testcases',
                      help='Directory to save the output workload files')
    parser.add_argument('--num-cases', type=int, default=50,
                      help='Number of test cases to generate')
    parser.add_argument('--seed', type=int, default=None,
                      help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed)
    
    generate_batch_workloads(args.topology, args.output, args.num_cases)
