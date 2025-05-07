import pandas as pd
from dataclasses import dataclass

@dataclass
class Link:
    link_id: int # 对链路编号，方便调度时表示（从 1 开始）
    src: int
    dst: int
    capacity: float # Gbps
    
class Graph:
    def __init__(self):
        self.link_num = 0
        self.nodes: set[int] = set() # node_id
        self.edges: dict[int, list[Link]] = {}  # src_id -> list[Link]
        
    def add_node(self, node_id: int) -> None:
        if node_id not in self.nodes:
            self.nodes.add(node_id)
            self.edges[node_id] = []

    def add_edge(self, src: int, dst: int, capacity: float) -> None:
        self.add_node(src)
        self.add_node(dst)
        
        link = Link(self.link_num, src, dst, capacity)
        self.link_num += 1
        self.edges[src].append(link)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> 'Graph':
        # 根据读入文件构建 Graph
        graph = cls()
        
        for _, row in df.iterrows():
            src = int(row['a_node_id'])
            dst = int(row['z_node_id'])
            capacity = float(row['bw(GBps)'])
            
            graph.add_edge(src, dst, capacity)
            
        return graph
