import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import argparse
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.use('Agg') # 非交互式后端

parser = argparse.ArgumentParser(description='Plotting script')
parser.add_argument('--policy', type=str, default='Ours', choices=['Ours', 'Greedy', 'NCFlow', 'IGR'], help='Policy to plot')
args = parser.parse_args()
policy = args.policy

# 计算CDF
def calculate_cdf(data):
    data_sorted = sorted(data)
    # 计算经验累积分布函数
    y_values = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
    return data_sorted, y_values

utilization = []
with open(f'figure/Phase2-{policy}链路利用率.txt', 'r') as f:
    for line in f:
        if line.strip():
            n = float(line.strip())
            if n == 0 or n > 1:
                continue
            utilization.append(n)

utilization_sorted, utilization_cdf = calculate_cdf(utilization)

fig, ax1 = plt.subplots(figsize=(6, 3))
weights = np.ones_like(utilization) / len(utilization)
hist_vals, bins, patches = ax1.hist(utilization, bins=40, weights=weights, ec='black',
         alpha=0.5, label='Probability')
ax1.set_ylabel('Probability Distribution', fontsize=10)
ax1.set_ylim(0, max(hist_vals) * 1.2)

ax2 = ax1.twinx()
ax2.plot(utilization_sorted, utilization_cdf, label='CDF', linewidth=2)
ax2.set_ylabel('CDF', fontsize=10)
ax2.set_ylim(0, 1.05)

ax1.set_title(f'{policy}: CDF of Link Utilization', fontsize=10)
ax1.set_xlabel('Link Utilization', fontsize=10)
ax1.set_xlim(0, 1.1)
ax1.grid(True, linestyle='--', alpha=0.7)
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, fontsize=10, loc='lower right')
plt.savefig(f'figure/Phase2-{policy}链路利用率CDF.pdf', dpi=300, bbox_inches='tight')
