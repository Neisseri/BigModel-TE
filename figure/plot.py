import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import random
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.use('Agg') # 非交互式后端

# fig 1-1：准入率相对于任务数的折线图
Ours_admit1 = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.8889, 1.0, 1.0, 0.8333, 1.0, 0.9286, 0.8, 0.6875, 0.7647, 0.7222, 0.7895, 0.7, 0.619, 0.5455, 0.7391, 0.6667, 0.48, 0.7308, 0.4815, 0.5357, 0.5517, 0.7, 0.5161, 0.5, 0.3636, 0.4706, 0.5429, 0.5833, 0.3243, 0.4474, 0.4615, 0.55, 0.4878, 0.4048, 0.3953, 0.3864, 0.3111, 0.3696, 0.4043, 0.3542, 0.3061, 0.26]
BATE_admit1 = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.7778, 0.9, 0.9091, 0.8333, 0.9231, 0.5714, 0.6667, 0.4375, 0.5294, 0.5, 0.6316, 0.45, 0.4762, 0.4545, 0.5217, 0.4167, 0.32, 0.3462, 0.2963, 0.3929, 0.3793, 0.3333, 0.2903, 0.2188, 0.2121, 0.1765, 0.2571, 0.25, 0.2973, 0.1842, 0.2051, 0.35, 0.2927, 0.2857, 0.2093, 0.2273, 0.2, 0.1957, 0.2553, 0.1667, 0.2245, 0.16]
Aequitas_admit1 = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.6667, 0.9, 0.8182, 0.75, 0.8462, 0.5, 0.7333, 0.4375, 0.5294, 0.5, 0.5789, 0.4, 0.4286, 0.4545, 0.4348, 0.375, 0.4, 0.4231, 0.2222, 0.3929, 0.3793, 0.3, 0.2581, 0.1875, 0.1818, 0.1471, 0.2286, 0.2222, 0.2703, 0.1842, 0.2051, 0.35, 0.2683, 0.2857, 0.2093, 0.1818, 0.2, 0.1739, 0.234, 0.1875, 0.2041, 0.14]
Seawall_admit1 = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.8889, 1.0, 0.7273, 0.9167, 0.6154, 0.5714, 0.8, 0.4375, 0.6471, 0.5556, 0.5263, 0.35, 0.4286, 0.2727, 0.3913, 0.375, 0.4, 0.1923, 0.2593, 0.2857, 0.3103, 0.3333, 0.3226, 0.3438, 0.303, 0.2941, 0.3429, 0.25, 0.1622, 0.2895, 0.2308, 0.275, 0.2683, 0.2381, 0.1395, 0.2727, 0.2444, 0.2609, 0.2553, 0.2708, 0.1224, 0.16]
jobs_num = range(1, 51)

plt.figure(figsize=(6, 3))
plt.plot(jobs_num, Ours_admit1, label='Ours')
plt.plot(jobs_num, BATE_admit1, label='BATE')
plt.plot(jobs_num, Aequitas_admit1, label='Aequitas')
plt.plot(jobs_num, Seawall_admit1, label='Seawall')
plt.title('Admission rate vs number of jobs', fontsize=10)
plt.xlabel('Jobs Number', fontsize=10)
plt.ylabel('Admission Rate', fontsize=10)
plt.xlim(min(jobs_num)-1, max(jobs_num)+1)
plt.ylim(0, 1.1)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10)
plt.savefig('figure/Phase1-准入率vs任务数.pdf', dpi=300, bbox_inches='tight')

# fig 1-2：准入率的累积分布函数图
Ours_admit2 = [0.6897, 0.5, 0.6364, 0.5172, 0.3636, 1.0, 0.75, 0.6, 0.6364, 0.619, 0.7, 1.0, 0.5667, 0.75, 0.4348, 0.5294, 0.6667, 0.5, 0.4444, 0.3333, 0.7308, 0.6429, 0.9231, 0.5667, 0.3, 0.4211, 0.9167, 0.5238, 0.5556, 0.6667, 0.7692, 0.5882, 0.8462, 0.4167, 0.4643, 0.5, 0.2857, 0.7368, 0.5385, 0.5185, 1.0, 0.4667, 0.6, 0.8333, 0.7647, 0.7273, 0.8889, 0.9, 0.5, 0.9]
BATE_admit2 = [0.2759, 0.25, 0.2273, 0.1724, 0.2273, 0.9167, 0.25, 0.16, 0.3636, 0.4286, 0.15, 1.0, 0.2667, 0.5833, 0.3478, 0.3529, 0.5, 0.375, 0.4444, 0.1429, 0.3462, 0.4286, 0.5385, 0.3, 0.1333, 0.2105, 0.75, 0.1429, 0.3889, 0.4167, 0.7692, 0.2941, 0.7692, 0.1667, 0.2143, 0.2917, 0.2857, 0.4737, 0.3077, 0.3333, 0.8, 0.2, 0.4, 0.4167, 0.3529, 0.4545, 0.5556, 0.7, 0.3077, 0.2]
Aequitas_admit2 = [0.3103, 0.3, 0.3182, 0.1724, 0.2273, 0.8333, 0.6, 0.2, 0.3636, 0.4762, 0.4, 0.75, 0.2667, 0.5, 0.3478, 0.2941, 0.3333, 0.375, 0.3333, 0.1429, 0.3077, 0.5714, 0.6154, 0.3667, 0.2667, 0.2632, 0.6667, 0.2857, 0.4444, 0.3333, 0.6154, 0.2941, 0.5385, 0.1667, 0.2143, 0.25, 0.2857, 0.3684, 0.3077, 0.3333, 0.8, 0.2333, 0.4667, 0.4167, 0.3529, 0.3636, 0.5556, 0.7, 0.2308, 0.3]
Seawall_admit2 = [0.5862, 0.3, 0.6364, 0.4138, 0.3182, 0.9167, 0.7, 0.44, 0.5455, 0.5238, 0.55, 0.9167, 0.5, 0.5833, 0.4348, 0.4118, 0.5833, 0.5, 0.3333, 0.3333, 0.5, 0.4286, 0.7692, 0.5, 0.4, 0.2632, 0.75, 0.4286, 0.4444, 0.5417, 0.6154, 0.4118, 0.6154, 0.25, 0.2857, 0.4583, 0.2143, 0.5789, 0.3077, 0.4815, 0.9, 0.4333, 0.5333, 0.5, 0.7059, 0.6364, 0.7778, 0.9, 0.4615, 0.8]
# 计算各策略的CDF
def calculate_cdf(data):
    data_sorted = sorted(data)
    # 计算经验累积分布函数
    y_values = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
    return data_sorted, y_values
ours_sorted, ours_cdf = calculate_cdf(Ours_admit2)
bate_sorted, bate_cdf = calculate_cdf(BATE_admit2)
aequitas_sorted, aequitas_cdf = calculate_cdf(Aequitas_admit2)
seawall_sorted, seawall_cdf = calculate_cdf(Seawall_admit2)

plt.figure(figsize=(6, 3))
plt.plot(ours_sorted, ours_cdf, label='Ours', linewidth=2)
plt.plot(bate_sorted, bate_cdf, label='BATE', linewidth=2)
plt.plot(aequitas_sorted, aequitas_cdf, label='Aequitas', linewidth=2)
plt.plot(seawall_sorted, seawall_cdf, label='Seawall', linewidth=2)
plt.title('CDF of Admission Rate', fontsize=10)
plt.xlabel('Admission Rate', fontsize=10)
plt.ylabel('CDF', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 1.05)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Phase1-准入率CDF.pdf', dpi=300, bbox_inches='tight')

# fig 1-3：Ours(SJF)链路利用率
# Ours(SJF)链路利用率.txt 中每行的 float 表示一条链路的负载率
ours_utilization = []
with open('figure/Ours(SJF)链路利用率.txt', 'r') as f:
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
plt.hist(ours_utilization, bins=40, ec = 'black', density=True, alpha=0.5, label='Link Utilization')
plt.plot(ours_utilization_sorted, ours_utilization_cdf, label='Link Utilization CDF', linewidth=2)
plt.title('Ours: CDF of Link Utilization', fontsize=10)
plt.xlabel('Link Utilization', fontsize=10)
plt.ylabel('CDF', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 1.05)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Ours链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# fig 1-4：BATE(FCFS)链路利用率
bate_utilization = []
with open('figure/BATE(FCFS)链路利用率.txt', 'r') as f:
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
plt.savefig('figure/BATE链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# fig 1-5：Aequitas(FCFS)链路利用率
aequitas_utilization = []
with open('figure/Aequitas(FCFS)链路利用率.txt', 'r') as f:
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
plt.savefig('figure/Aequitas链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# fig 1-6：Seawall(FCFS)链路利用率
seawall_utilization = []
with open('figure/Seawall(FCFS)链路利用率.txt', 'r') as f:
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
plt.savefig('figure/Seawall链路利用率CDF.pdf', dpi=300, bbox_inches='tight')

# 计算链路负载均衡指标
def calculate_balance_metrics(utilization_data):
    # 排除零值(没有使用的链路)
    non_zero_data = [x for x in utilization_data if x > 0]

    # 1. 标准差 - 值越低表示负载分布越均衡
    std_dev = np.std(non_zero_data)
    # 2. 变异系数 (CV) - 标准差除以平均值，值越低表示分布越均衡
    mean = np.mean(non_zero_data)
    cv = std_dev / mean if mean > 0 else 0
    # 3. 计算基尼系数 - 值越接近0表示分布越均衡
    sorted_data = sorted(non_zero_data)
    n = len(sorted_data)
    index = np.arange(1, n+1)
    gini = (2 * np.sum(index * sorted_data) / (n * np.sum(sorted_data))) - (n + 1) / n
    # 4. Jain's公平指数 - 值越接近1表示分布越均衡
    sum_x = sum(non_zero_data)
    sum_x_squared = sum(x**2 for x in non_zero_data)
    jain = (sum_x**2) / (n * sum_x_squared) if sum_x_squared > 0 else 1
    
    return {
        "标准差": std_dev,
        "变异系数": cv,
        "基尼系数": gini,
        "Jain公平指数": jain
    }

# 计算每种策略的负载均衡指标
print("\n===== 链路负载均衡指标比较 =====")
ours_metrics = calculate_balance_metrics(ours_utilization)
bate_metrics = calculate_balance_metrics(bate_utilization)
aequitas_metrics = calculate_balance_metrics(aequitas_utilization)
seawall_metrics = calculate_balance_metrics(seawall_utilization)
print("\n标准差 (越低越均衡):")
print(f"Ours(SJF): {ours_metrics['标准差']:.4f}")
print(f"BATE(FCFS): {bate_metrics['标准差']:.4f}")
print(f"Aequitas(FCFS): {aequitas_metrics['标准差']:.4f}")
print(f"Seawall(FCFS): {seawall_metrics['标准差']:.4f}")
print("\n变异系数 (越低越均衡):")
print(f"Ours(SJF): {ours_metrics['变异系数']:.4f}")
print(f"BATE(FCFS): {bate_metrics['变异系数']:.4f}")
print(f"Aequitas(FCFS): {aequitas_metrics['变异系数']:.4f}")
print(f"Seawall(FCFS): {seawall_metrics['变异系数']:.4f}")
print("\n基尼系数 (越接近0越均衡):")
print(f"Ours(SJF): {ours_metrics['基尼系数']:.4f}")
print(f"BATE(FCFS): {bate_metrics['基尼系数']:.4f}")
print(f"Aequitas(FCFS): {aequitas_metrics['基尼系数']:.4f}")
print(f"Seawall(FCFS): {seawall_metrics['基尼系数']:.4f}")
print("\nJain公平指数 (越接近1越均衡):")
print(f"Ours(SJF): {ours_metrics['Jain公平指数']:.4f}")
print(f"BATE(FCFS): {bate_metrics['Jain公平指数']:.4f}")
print(f"Aequitas(FCFS): {aequitas_metrics['Jain公平指数']:.4f}")
print(f"Seawall(FCFS): {seawall_metrics['Jain公平指数']:.4f}")

# fig 1-7：准入任务启动时间/周期的概率分布
# jobs_start_time = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.4, 0.0, 0.0847457627118644, 0.12658227848101267, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5769230769230769, 0.6666666666666666, 0.0, 0.1, 0.0784313725490196, 0.0, 0.0, 0.08196721311475409, 0.0, 0.0, 0.12269938650306748, 0.3053435114503817, 0.0, 0.0, 0.39473684210526316, 0.0, 0.0, 0.7746478873239436, 0.0, 0.0, 0.0, 0.0, 0.6666666666666666, 0.15625, 0.5244755244755245, 0.0, 0.0, 0.8148148148148148, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.03861003861003861, 0.12121212121212122, 0.0, 0.0, 0.0, 0.2564102564102564, 0.0, 0.0, 0.6481481481481481, 0.0, 0.33557046979865773, 0.0, 0.04149377593360996, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.17857142857142858, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.6451612903225806, 0.0, 0.0, 0.0, 0.2777777777777778, 0.11834319526627218, 0.0, 0.0, 0.0, 0.0, 0.0, 0.11627906976744186, 0.0, 0.0, 0.0, 0.0, 0.3448275862068966, 0.0, 0.0, 0.12658227848101267, 0.0, 0.0, 0.0, 0.036231884057971016, 0.0, 0.3225806451612903, 0.0, 0.0, 0.0, 0.046511627906976744, 0.0, 0.2777777777777778, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.7777777777777778, 0.29850746268656714, 0.1652892561983471, 0.0, 0.0, 0.0, 0.10309278350515463, 0.45977011494252873, 0.09389671361502347, 0.0, 0.0, 0.32608695652173914, 0.0, 0.0, 0.125, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.09174311926605505, 0.3488372093023256, 0.0, 0.2702702702702703, 0.4020100502512563, 0.0, 0.06493506493506493, 0.0, 0.0, 0.0, 0.0, 0.0, 0.2347417840375587, 0.0, 0.08888888888888889, 0.0, 0.03355704697986577, 0.0, 0.21818181818181817, 0.0, 0.0, 0.19607843137254902, 0.7103825136612022, 0.19157088122605365, 0.0851063829787234, 0.0, 0.0, 0.2857142857142857, 0.041666666666666664, 0.39473684210526316, 0.0, 0.0, 0.7377049180327869, 0.0, 0.29411764705882354, 0.0, 0.703125, 0.0, 0.0, 0.06802721088435375, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.07722007722007722, 0.13215859030837004, 0.0, 0.851063829787234, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8823529411764706, 0.0, 0.30303030303030304, 0.9433962264150944, 0.0, 0.0, 0.0, 0.0, 0.04310344827586207, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.7070707070707071, 0.46296296296296297, 0.47058823529411764, 0.14018691588785046, 0.0, 0.11299435028248588, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.19230769230769232, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.05025125628140704, 0.0, 0.0, 0.0, 0.0, 0.03496503496503497, 0.0, 0.43478260869565216, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.03636363636363636, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.09259259259259259, 0.03861003861003861, 0.75, 0.20618556701030927, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5179282868525896, 0.0, 0.989010989010989, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.4065040650406504, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.041666666666666664, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
jobs_start_time_raw = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.30303030303030304, 0.0, 0.4878048780487805, 0.16666666666666666, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.21505376344086022, 0.2564102564102564, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8695652173913043, 0.9090909090909091, 1.6304347826086956, 0.0, 0.0, 2.8205128205128207, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.19607843137254902, 0.25316455696202533, 0.0, 0.0, 0.0, 0.20202020202020202, 0.0, 0.0, 2.3076923076923075, 0.0, 0.0, 0.0, 0.12048192771084337, 0.2127659574468085, 0.0, 0.0, 0.0, 0.0, 0.0, 0.37037037037037035, 0.0, 0.0, 0.0, 0.0, 0.3076923076923077, 0.0, 0.0, 0.11627906976744186, 0.0, 0.0, 0.0, 0.10869565217391304, 0.0, 0.2127659574468085, 0.0, 0.0, 0.0, 0.15384615384615385, 0.0, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.028985507246377, 0.41237113402061853, 0.4878048780487805, 0.0, 0.0, 0.0, 0.46153846153846156, 1.2307692307692308, 0.5714285714285714, 0.0, 0.0, 1.3043478260869565, 0.0, 0.0, 0.22727272727272727, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.10638297872340426, 0.759493670886076, 0.0, 0.36363636363636365, 3.4782608695652173, 0.0, 0.24390243902439024, 0.0, 0.0, 0.0, 0.0, 0.0, 1.1627906976744187, 0.0, 0.5128205128205128, 0.0, 0.16129032258064516, 0.0, 1.875, 0.0, 0.0, 0.29411764705882354, 1.5116279069767442, 0.8928571428571429, 0.20618556701030927, 0.0, 0.0, 0.20833333333333334, 0.4, 0.5084745762711864, 0.0, 0.0, 3.5294117647058822, 0.0, 0.5208333333333334, 0.0, 6.666666666666667, 0.0, 0.0, 0.30303030303030304, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.273972602739726, 0.6382978723404256, 0.0, 0.45977011494252873, 0.0, 0.0, 0.0, 0.0, 0.0, 1.7647058823529411, 0.0, 0.847457627118644, 1.9230769230769231, 0.0, 0.0, 0.0, 0.0, 0.12658227848101267, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.9166666666666665, 1.1111111111111112, 1.4814814814814814, 0.3125, 0.0, 0.3333333333333333, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.11764705882352941, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 0.0, 0.0, 0.3448275862068966, 0.0, 0.625, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.18518518518518517, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.18867924528301888, 0.10309278350515463, 0.7692307692307693, 0.39215686274509803, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 4.333333333333333, 0.0, 3.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.6756756756756757, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.37037037037037035, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.16666666666666666, 0.0, 0.2702702702702703, 0.1388888888888889, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.2564102564102564, 0.21052631578947367, 0.0, 0.0, 0.0, 0.0, 0.0, 1.1428571428571428, 0.45454545454545453, 3.1914893617021276, 0.0, 0.0, 1.5277777777777777, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.12658227848101267, 0.273972602739726, 0.0, 0.0, 0.0, 0.7692307692307693, 0.0, 0.0, 0.8333333333333334, 0.0, 0.0, 0.0, 0.3333333333333333, 0.37735849056603776, 0.0, 0.0, 0.0, 0.0, 0.0, 1.5, 0.0, 0.0, 0.0, 0.0, 0.2597402597402597, 0.0, 0.0, 0.16129032258064516, 0.0, 0.0, 0.0, 0.10204081632653061, 0.0, 0.2222222222222222, 0.0, 0.0, 0.0, 0.16666666666666666, 0.0, 0.37037037037037035, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.75, 0.6349206349206349, 0.6666666666666666, 0.0, 0.0, 0.0, 1.3043478260869565, 0.975609756097561, 0.3125, 0.0, 0.0, 1.25, 0.0, 0.0, 0.13333333333333333, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.10101010101010101, 1.0909090909090908, 0.0, 0.21739130434782608, 1.0, 0.0, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.625, 0.0, 0.32786885245901637, 0.0, 0.1694915254237288, 0.0, 0.8695652173913043, 0.0, 0.0, 0.20833333333333334, 1.8840579710144927, 0.6493506493506493, 0.20618556701030927, 0.0, 0.0, 0.16666666666666666, 0.125, 0.8333333333333334, 0.0, 0.0, 2.4, 0.0, 0.6944444444444444, 0.0, 3.673469387755102, 0.0, 0.0, 0.47619047619047616, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.2564102564102564, 0.5882352941176471, 0.0, 0.40816326530612246, 0.0, 0.0, 0.0, 0.0, 0.0, 2.3076923076923075, 0.0, 1.7857142857142858, 1.1363636363636365, 0.0, 0.0, 0.0, 0.0, 0.1282051282051282, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.8421052631578947, 1.0, 1.6, 0.4411764705882353, 0.0, 0.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.10989010989010989, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.4, 0.0, 0.0, 0.0, 0.0, 0.13157894736842105, 0.0, 0.5555555555555556, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.4166666666666667, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.1282051282051282, 0.13513513513513514, 0.4166666666666667, 0.2247191011235955, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.4210526315789473, 0.0, 0.9473684210526315, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5208333333333334, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.47619047619047616, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
jobs_start_time = []
for j in jobs_start_time_raw:
    if j > 0:
        jobs_start_time.append(j)
for idx, _ in enumerate(jobs_start_time):
    while jobs_start_time[idx] >= 1:
        jobs_start_time[idx] -= 1      

# 计算概率密度函数
def calculate_pdf(data):
    data_sorted = sorted(data)
    # 计算概率密度函数
    pdf, bins = np.histogram(data_sorted, bins=40, density=True)
    print(pdf)
    bin_centers = 0.5 * (bins[1:] + bins[:-1])
    return bin_centers, pdf
jobs_start_time_sorted, jobs_start_time_pdf = calculate_pdf(jobs_start_time)

plt.figure(figsize=(8, 4))
plt.hist(jobs_start_time, bins=40, ec = 'black', density=True, alpha=0.5, label='Jobs Start Time')
plt.title('PDF of Jobs Start Time', fontsize=10)
plt.xlabel('Jobs Start Time', fontsize=10)
plt.ylabel('Probability Density', fontsize=10)
plt.xlim(0, 1.1)
plt.ylim(0, 3.5)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=10, loc='lower right')
plt.savefig('figure/Phase1-任务启动时间PDF.pdf', dpi=300, bbox_inches='tight')