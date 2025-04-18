import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import json
import numpy as np
from math import gcd
from functools import reduce
import argparse

def lcm(a, b):
    return abs(a * b) // gcd(a, b)

def lcm_multiple(numbers):
    return reduce(lcm, numbers)

def load_data(result_file: str, workload_file: str):
    with open(result_file, 'r') as f:
        results = json.load(f)
    with open(workload_file, 'r') as f:
        workload = json.load(f)
    return results, workload

def create_timeline_plot(results, workload, figure):
    plt.figure(figsize=(15, 8))
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # 只计算成功调度的任务的周期最小公倍数
    cycles = []
    for job in results:
        job_idx = job['job_id']
        if job['status'] == 'success':
            cycles.append(workload[job_idx]['cycle(ms)'])
    
    total_cycle = lcm_multiple(cycles) if cycles else 0
    time_points = np.arange(0, total_cycle, 1)  # 加100ms作为缓冲
    total_bw = np.zeros(len(time_points))
    max_time = 0
    
    for job in results:
        job_idx = job['job_id']
        if job['status'] != 'success':
            continue
            
        job_bw = np.zeros(len(time_points))
        start_time = job['start_time']
        
        if 'demands' not in job:
            continue
        
        # 从 workload 中获取对应作业的循环时间
        job_cycle = workload[job_idx]['cycle(ms)']
        
        for demand in job['demands']:
            # 根据 demand_id 获取对应的 workload demand 信息
            print('demand[\'demand_id\']:', demand['demand_id'])
            workload_demand = workload[job_idx]['demands'][demand['demand_id']]
            demand_start = workload_demand['start_timestamp(ms)']
            demand_end = workload_demand['end_timestamp(ms)']

            print(f"Job {job_idx} demand {demand['demand_id']} start: {demand_start}, end: {demand_end}")
            
            bw = demand['paths'][0]['bandwidth']
                
            # 计算实际的开始和结束时间点
            for t_idx, t in enumerate(time_points):
                t_in_cycle = (t - start_time) % job_cycle
                if demand_start <= t_in_cycle < demand_end:
                    job_bw[t_idx] += bw
        
        # 绘制这个作业的带宽使用
        plt.plot(time_points, job_bw, color=colors[job_idx], 
                label=f'Job {job["job_id"]}', linewidth=2, alpha=0.7)
        total_bw += job_bw
        max_time = max(max_time, max(time_points[job_bw > 0]))
    
    # 绘制总带宽使用
    plt.plot(time_points, total_bw, color='gray', 
            label='Total Bandwidth', linewidth=2, linestyle='--', alpha=0.5)
    
    # 设置图表属性
    plt.xlabel('Time (ms)', fontsize=12)
    plt.ylabel('Bandwidth (Gbps)', fontsize=12)
    plt.title('Job Schedule Timeline', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10, loc='upper right')
    
    # 调整x轴显示范围
    plt.xlim(0, min(total_cycle, max_time + 200))
    
    # 保存图表
    plt.savefig(figure, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    # python3 src/visualize_schedule.py --result figure_data/2_job_0.json --workload multi_job/workload_2_job.json --output figure_data/2_job_0.png
    # python3 src/visualize_schedule.py --result figure_data/2_job_1.json --workload multi_job/workload_2_job.json --output figure_data/2_job_1.png
    # python3 src/visualize_schedule.py --result figure_data/2_job_baseline.json --workload multi_job/workload_2_job.json --output figure_data/2_job_baseline.png
    # python3 src/visualize_schedule.py --result figure_data/2_job_geometric.json --workload multi_job/workload_2_job.json --output figure_data/2_job_geometric.png

    # python3 src/visualize_schedule.py --result figure_data/4_job_0.json --workload multi_job/workload_4_job.json --output figure_data/4_job_0.png
    # python3 src/visualize_schedule.py --result figure_data/4_job_1.json --workload multi_job/workload_4_job.json --output figure_data/4_job_1.png
    # python3 src/visualize_schedule.py --result figure_data/4_job_2.json --workload multi_job/workload_4_job.json --output figure_data/4_job_2.png
    # python3 src/visualize_schedule.py --result figure_data/4_job_3.json --workload multi_job/workload_4_job.json --output figure_data/4_job_3.png
    # python3 src/visualize_schedule.py --result figure_data/4_job_baseline.json --workload multi_job/workload_4_job.json --output figure_data/4_job_baseline.png
    # python3 src/visualize_schedule.py --result figure_data/4_job_geometric.json --workload multi_job/workload_4_job.json --output figure_data/4_job_geometric.png
    parser = argparse.ArgumentParser(description='Visualize job schedule timeline')
    parser.add_argument('--result', type=str, required=True,
                      help='Path to the result JSON file')
    parser.add_argument('--workload', type=str, required=True,
                      help='Path to the workload JSON file')
    parser.add_argument('--output', type=str, required=True,
                      help='Path to save the output figure')
    
    args = parser.parse_args()
    
    results, workload = load_data(args.result, args.workload)
    create_timeline_plot(results, workload, args.output)
    print(f"Visualization has been saved to {args.output}")
