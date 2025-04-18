from dataclasses import dataclass

@dataclass
class Demand:
    job_id: int
    src_rank: int
    dst_rank: int
    start_time: float  # milliseconds
    end_time: float    # milliseconds
    bandwidth: float   # Gbps
