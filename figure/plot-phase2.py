import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import random
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.use('Agg') # 非交互式后端

# Ours
ours_utilization = []
with open('figure/Phase2-Ours链路利用率.txt', 'r') as f:
    for line in f:
        if line.strip():
            n = float(line.strip())
            if n == 0 or n > 1:
                continue
            ours_utilization.append(n)
for i in range(len(ours_utilization)):
    if ours_utilization[i] > 1:
        ours_utilization[i] = 1
# 计算CDF
def calculate_cdf(data):
    data_sorted = sorted(data)
    # 计算经验累积分布函数
    y_values = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
    return data_sorted, y_values
ours_utilization_sorted, ours_utilization_cdf = calculate_cdf(ours_utilization)

plt.figure(figsize=(6, 3))
weights = np.ones_like(ours_utilization) / len(ours_utilization)
plt.hist(ours_utilization, bins=40, weights=weights, ec='black',
         alpha=0.5, label='Link Utilization')
# plt.hist(ours_utilization, bins=40, ec = 'black', density=True, alpha=0.5, label='Link Utilization')
plt.plot(ours_utilization_sorted, ours_utilization_cdf, label='Link Utilization CDF', linewidth=2)
plt.title('Ours: CDF of Link Utilization', fontsize=10)
plt.xlabel('Link Utilization', fontsize=10)
plt.ylabel('CDF', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 1.05)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Phase2-Ours链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# Greedy
bate_utilization = []
with open('figure/Phase2-Greedy链路利用率.txt', 'r') as f:
    for line in f:
        if line.strip():
            n = float(line.strip())
            if n == 0 or n > 1:
                continue
            bate_utilization.append(n)
for i in range(len(ours_utilization)):
    if ours_utilization[i] > 1:
        ours_utilization[i] = 1
bate_utilization_sorted, bate_utilization_cdf = calculate_cdf(bate_utilization)

plt.figure(figsize=(6, 3))
plt.hist(bate_utilization, bins=40, ec = 'black', density=True, alpha=0.5, label='Link Utilization')
plt.plot(bate_utilization_sorted, bate_utilization_cdf, label='Link Utilization CDF', linewidth=2)
plt.title('BATE: CDF of Link Utilization', fontsize=10)
plt.xlabel('Link Utilization', fontsize=10)
plt.ylabel('CDF', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 1.05)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Phase2-Greedy链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# fig 1-5：Aequitas(FCFS)链路利用率
aequitas_utilization = []
with open('figure/Phase2-NCFlow链路利用率.txt', 'r') as f:
    for line in f:
        if line.strip():
            n = float(line.strip())
            if n == 0 or n > 1:
                continue
            aequitas_utilization.append(n)
for i in range(len(ours_utilization)):
    if ours_utilization[i] > 1:
        ours_utilization[i] = 1
aequitas_utilization_sorted, aequitas_utilization_cdf = calculate_cdf(aequitas_utilization)

plt.figure(figsize=(6, 3))
plt.hist(aequitas_utilization, bins=40, ec = 'black', density=True, alpha=0.5, label='Link Utilization')
plt.plot(aequitas_utilization_sorted, aequitas_utilization_cdf, label='Link Utilization CDF', linewidth=2)
plt.title('Aequitas: CDF of Link Utilization', fontsize=10)
plt.xlabel('Link Utilization', fontsize=10)
plt.ylabel('CDF', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 1.05)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Phase2-NCFlow链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# fig 1-6：Seawall(FCFS)链路利用率
seawall_utilization = []
with open('figure/Phase2-IGR链路利用率.txt', 'r') as f:
    for line in f:
        if line.strip():
            n = float(line.strip())
            if n == 0 or n > 1:
                continue
            seawall_utilization.append(n)
for i in range(len(ours_utilization)):
    if ours_utilization[i] > 1:
        ours_utilization[i] = 1
seawall_utilization_sorted, seawall_utilization_cdf = calculate_cdf(seawall_utilization)

plt.figure(figsize=(6, 3))
plt.hist(seawall_utilization, bins=40, ec = 'black', density=True, alpha=0.5, label='Link Utilization')
plt.plot(seawall_utilization_sorted, seawall_utilization_cdf, label='Link Utilization CDF', linewidth=2)
plt.title('Seawall: CDF of Link Utilization', fontsize=10)
plt.xlabel('Link Utilization', fontsize=10)
plt.ylabel('CDF', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 1.05)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Phase2-IGR链路利用率CDF.pdf', dpi=300, bbox_inches='tight')