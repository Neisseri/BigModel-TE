import gurobipy as gp
from gurobipy import GRB
from dataclasses import dataclass
import numpy as np
from typing import Optional
import copy
import sys
import os
from job.job_info import JobInfo

# 动态添加项目根目录到 sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from network.graph import Graph, Link
from network.path_finder import PathFinder
from params import SCHEDULE_INTERVAL

# 基于 BATE 中的准入控制策略实现

class FCFS():

    def __init__(self, network: Graph):

        self.network: Graph = network


    # def add_traffic(self, link_id: int, traffic: Traffic) -> None:
    
    # def direct_deploy(self, job: JobInfo) -> int:
        
    #     job_id = job.job_id
    #     self.jobs[job_id] = job
    #     self.job_schedules[job_id] = JobSchedule(
    #         admit = 0,
    #         start_time = 0,
    #         tunnels = [],
    #         bw_alloc = []
    #     )