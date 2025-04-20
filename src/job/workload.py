from dataclasses import dataclass

@dataclass
class Workload:
    src: int
    dst: int
    t_s: int # 开始时间（ms）
    t_e: int # 结束时间（ms）
    bw: float # 带宽（Gbps）
