import matplotlib
matplotlib.use('Agg')
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
from scipy import stats
from matplotlib.font_manager import FontProperties
import json

# 设置全局样式
plt.style.use('default')  # 使用默认样式
plt.rcParams.update({
    'figure.figsize': (12, 7),
    'font.size': 12,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'grid.alpha': 0.3,
    'lines.linewidth': 2,
    'axes.grid': True,
    'grid.linestyle': '--'
})

def plot_bandwidth_comparison(df: pd.DataFrame, output_dir: str):
    plt.figure(figsize=(12, 6))
    
    # 按任务数排序
    df_sorted = df.sort_values('job_num')
    
    plt.plot(df_sorted['job_num'], df_sorted['baseline_total_bw'], 'o-', 
             label='Baseline', color='#2077B4', alpha=0.8)
    plt.plot(df_sorted['job_num'], df_sorted['scheduling_total_bw'], 'o-', 
             label='Strategy 2', color='#FF7F0E', alpha=0.8)
    
    plt.xlabel('Number of Jobs')
    plt.ylabel('Total Bandwidth (Gbps)')
    plt.title('Bandwidth Comparison vs Number of Jobs')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'bandwidth_comparison.png'), dpi=300, bbox_inches='tight')
    plt.close()

def plot_improvement_bars(df: pd.DataFrame, output_dir: str):
    plt.figure(figsize=(12, 6))
    
    # 按任务数排序
    df_sorted = df.sort_values('job_num')
    
    bars = plt.bar(df_sorted['job_num'], df_sorted['improvement'], alpha=0.8, color='#2077B4')
    plt.xlabel('Number of Jobs')
    plt.ylabel('Optimization Rate (%)')
    plt.title('Bandwidth Optimization Rate vs Number of Jobs')
    
    # 添加数值标签
    for i, v in enumerate(df_sorted['improvement']):
        plt.text(df_sorted['job_num'].iloc[i], v, f'{v:.1f}%', 
                ha='center', va='bottom')
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'improvement_bars.png'), dpi=300, bbox_inches='tight')
    plt.close()

def plot_improvement_cdf(df: pd.DataFrame, output_dir: str):
    plt.figure(figsize=(8, 6))
    improvements = sorted(df['improvement'])
    
    # 计算累积分布
    y = np.arange(1, len(improvements) + 1) / len(improvements)
    
    plt.plot(improvements, y, '-', color='#2077B4', linewidth=2)
    plt.grid(True, alpha=0.3)
    plt.xlabel('Optimization Rate (%)')  # 改为英文
    plt.ylabel('Cumulative Probability')
    plt.title('CDF of Bandwidth Optimization Rate')
    
    # 添加均值线和标注
    mean_improvement = np.mean(improvements)
    plt.axvline(x=mean_improvement, color='red', linestyle='--', alpha=0.8)
    plt.text(mean_improvement + 0.2, 0.3, f'Mean: {mean_improvement:.2f}%', 
             ha='left', va='center')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'improvement_cdf.png'), dpi=300, bbox_inches='tight')
    plt.close()

def plot_improvement_trend(df: pd.DataFrame, output_dir: str):
    plt.figure(figsize=(12, 6))
    x = df['job_num']
    y = df['improvement']
    
    # 绘制散点图
    plt.scatter(x, y, alpha=0.6, color='#2077B4', label='Data Points')
    
    # 计算趋势线（二次拟合）
    z = np.polyfit(x, y, 2)
    p = np.poly1d(z)
    x_trend = np.linspace(min(x), max(x), 100)
    y_trend = p(x_trend)
    
    # 计算相关系数
    correlation = stats.pearsonr(x, y)[0]
    
    # 绘制趋势线
    plt.plot(x_trend, y_trend, 'r--', alpha=0.8, 
            label=f'Trend (R={correlation:.2f})')
    
    plt.xlabel('Number of Jobs')
    plt.ylabel('Optimization Rate (%)')
    plt.title('Bandwidth Optimization Rate vs Number of Jobs')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'improvement_trend.png'), dpi=300, bbox_inches='tight')
    plt.close()

def analyze_and_plot(data_file: str):
    # 读取数据
    df = pd.read_csv(data_file, sep=' ')
    
    # 创建输出目录
    output_dir = 'figure_data/figures'
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成图表
    plot_bandwidth_comparison(df, output_dir)
    plot_improvement_bars(df, output_dir)
    plot_improvement_cdf(df, output_dir)
    plot_improvement_trend(df, output_dir)  # 新增趋势图
    
    # 计算并显示统计信息
    improvements = df['improvement']
    print(f"\nStatistics:")
    print(f"Average improvement: {improvements.mean():.2f}%")
    print(f"Maximum improvement: {improvements.max():.2f}%")
    print(f"Minimum improvement: {improvements.min():.2f}%")
    print(f"Median improvement: {improvements.median():.2f}%")
    print(f"90th percentile: {np.percentile(improvements, 90):.2f}%")
    print(f"10th percentile: {np.percentile(improvements, 10):.2f}%")  # 修复：删除多余的冒号
    print(f"Standard deviation: {improvements.std():.2f}%")
    
    # 添加相关性分析
    correlation = stats.pearsonr(df['job_num'], df['improvement'])[0]
    print(f"\nCorrelation between job number and improvement: {correlation:.3f}")

if __name__ == '__main__':
    analyze_and_plot('figure_data/data.txt')

import matplotlib.pyplot as plt
import numpy as np

def load_results(result_dir: str, num_testcases: int = 50) -> dict:
    """从结果文件中加载所有算法的数据"""
    algorithms = {
        'TE': [],
        'A*': [],
        'PDA': [],
        'Greedy': []
    }
    
    # 追踪成功读取的测例数
    successful_cases = 0
    
    for i in range(1, num_testcases + 1):
        testcase_dir = os.path.join(result_dir, f'testcase{i}')
        if not os.path.exists(testcase_dir):
            continue
            
        case_valid = True
        case_data = {}
        
        # 读取各个算法的结果
        for algo in algorithms.keys():
            result_file = os.path.join(testcase_dir, f"{algo.lower()}_result.json")
            if not os.path.exists(result_file):
                case_valid = False
                break
                
            try:
                with open(result_file, 'r') as f:
                    data = json.load(f)
                    peak_bw = 0
                    for job in data:
                        if isinstance(job, dict) and job.get("status") == "success":
                            for demand in job.get("demands", []):
                                for path in demand.get("paths", []):
                                    peak_bw += path.get("bandwidth", 0)
                    case_data[algo] = peak_bw
            except (json.JSONDecodeError, KeyError, TypeError):
                case_valid = False
                break
        
        if case_valid and all(bw > 0 for bw in case_data.values()):
            successful_cases += 1
            for algo, bw in case_data.items():
                algorithms[algo].append(bw)
    
    if successful_cases == 0:
        raise ValueError("No valid data found in the result directory")
        
    return algorithms

def parse_md_file(file_path: str) -> dict:
    """从 markdown 文件中解析带宽数据"""
    algorithms = {
        'TE': [],
        'A*': [],
        'PDA': [],
        'Greedy': []
    }
    
    current_case = None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('testcase'):
                current_case = int(line.split()[1])
            elif ':' in line:
                algo, value = line.split(':')
                algo = algo.strip()
                if algo in algorithms:
                    try:
                        bw = float(value.strip())
                        algorithms[algo].append(bw)
                    except ValueError:
                        continue
    
    return algorithms

def plot_bandwidth_comparison():
    plt.figure()
    
    try:
        # 从 markdown 文件加载数据
        results = parse_md_file("数据.md")
        algorithms = list(results.keys())
        all_results = [results[algo] for algo in algorithms]
        
        if not all(all_results):
            raise ValueError("Some algorithms have no valid results")
            
        means = [np.mean(res) for res in all_results]
        std_devs = [np.std(res) for res in all_results]
        
        # 修改柱状图样式
        x = np.arange(len(algorithms))
        bars = plt.bar(x, means, yerr=std_devs, capsize=5, 
                      color='#3498db',  # 统一使用同一种蓝色
                      width=0.6,
                      edgecolor='black', 
                      linewidth=2.0,  # 加粗边缘线
                      alpha=0.8)
        
        # 设置图表属性
        plt.title('Peak Bandwidth Comparison of Different Algorithms', pad=20)
        plt.xlabel('Scheduling Algorithms', labelpad=10)
        plt.ylabel('Peak Bandwidth (Gbps)', labelpad=10)
        plt.xticks(x, algorithms)
        
        # 在柱子上标注具体数值
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}',
                    ha='center', va='bottom',
                    fontweight='bold')
        
        # 添加误差线的标签
        plt.errorbar(x, means, yerr=std_devs, fmt='none', 
                    capsize=5, capthick=2, 
                    ecolor='black', elinewidth=2,
                    label='Standard Deviation')
        
        # 添加图例
        plt.legend(loc='upper right')
        
        # 调整布局并保存
        plt.tight_layout()
        plt.savefig('result/algorithm_comparison.png', 
                   dpi=300, bbox_inches='tight',
                   facecolor='white', edgecolor='none')
        
        print(f"Successfully generated comparison plot with {len(all_results[0])} test cases")
        
    except Exception as e:
        print(f"Error generating comparison plot: {str(e)}")
    finally:
        plt.close()

def plot_bandwidth_boxplot():
    plt.figure()
    
    try:
        # 从 markdown 文件加载数据
        results = parse_md_file("数据.md")
        data = [results[algo] for algo in results.keys()]
        labels = list(results.keys())
        
        if not all(data):
            raise ValueError("Some algorithms have no valid results")
        
        # 绘制箱型图
        plt.boxplot(data, labels=labels)
        
        # 设置图表属性
        plt.title('Distribution of Peak Bandwidth by Algorithm')
        plt.xlabel('Algorithms')
        plt.ylabel('Peak Bandwidth (Gbps)')
        
        # 调整布局并保存
        plt.tight_layout()
        plt.savefig('result/algorithm_distribution.png', dpi=300, bbox_inches='tight')
        print(f"Successfully generated distribution plot with {len(data[0])} test cases")
    except Exception as e:
        print(f"Error generating distribution plot: {str(e)}")
    finally:
        plt.close()

if __name__ == '__main__':
    # 确保结果目录存在
    os.makedirs('result', exist_ok=True)
    
    # 绘制平均带宽比较图
    plot_bandwidth_comparison()
    
    # 绘制带宽分布箱型图
    plot_bandwidth_boxplot()
    
    # 打印数据统计信息
    results = parse_md_file("数据.md")
    print("\nAlgorithm Statistics:")
    for algo, values in results.items():
        print(f"\n{algo}:")
        print(f"Mean: {np.mean(values):.2f}")
        print(f"Std: {np.std(values):.2f}")
        print(f"Min: {np.min(values):.2f}")
        print(f"Max: {np.max(values):.2f}")
    
    print("\nPlots have been saved to result/algorithm_comparison.png and result/algorithm_distribution.png")
