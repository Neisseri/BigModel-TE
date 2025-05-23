import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import random
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.use('Agg') # 非交互式后端
# 假设 ours_utilization 是你读取并处理后的数据
# 示例数据
ours_utilization = np.random.beta(2, 5, size=1000)  # 模拟数据

# CDF 计算函数
def calculate_cdf(data):
    data_sorted = np.sort(data)
    y_values = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
    return data_sorted, y_values

ours_utilization_sorted, ours_utilization_cdf = calculate_cdf(ours_utilization)

# 创建图形和两个 y 轴
fig, ax1 = plt.subplots(figsize=(6, 3))

# 画直方图（左边 y 轴）
num_bins = 40
weights = np.ones_like(ours_utilization) / len(ours_utilization)
hist_vals, bins, patches = ax1.hist(
    ours_utilization, bins=num_bins, weights=weights,
    ec='black', alpha=0.5, label='Link Utilization', color='skyblue'
)
ax1.set_ylabel('Probability (Histogram)', fontsize=10)
ax1.set_ylim(0, max(hist_vals) * 1.2)

# 创建右边的 y 轴用于画 CDF
ax2 = ax1.twinx()
ax2.plot(
    ours_utilization_sorted, ours_utilization_cdf,
    label='CDF', color='darkorange', linewidth=2
)
ax2.set_ylabel('CDF', fontsize=10)
ax2.set_ylim(0, 1.05)

# 公共设置
ax1.set_title('Link Utilization: Histogram + CDF', fontsize=10)
ax1.set_xlabel('Link Utilization', fontsize=10)
ax1.set_xlim(0, 1.1)
ax1.grid(True, linestyle='--', alpha=0.7)

# 图例处理
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, fontsize=10, loc='upper center')

# 保存图像
plt.tight_layout()
plt.savefig('figure/LinkUtilization_Hist_CDF_dualYaxis.pdf', dpi=300)
plt.show()
