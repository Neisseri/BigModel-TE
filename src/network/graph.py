from typing import Dict, List, Tuple, Set, Optional
import pandas as pd
from dataclasses import dataclass
from collections import deque

@dataclass
class Link:
    src_rank: int
    dst_rank: int
    src_type: str
    dst_type: str
    delay: float  # milliseconds
    bandwidth: float  # Gbps
    
class Graph:
    def __init__(self):
        self.nodes: Dict[int, str] = {}  # node_id -> node_type
        self.edges: Dict[int, List[Link]] = {}  # src_id -> List[Link]
        
    def add_node(self, node_id: int, node_type: str) -> None:
        if node_id not in self.nodes:
            self.nodes[node_id] = node_type
            self.edges[node_id] = []

    def add_edge(self, src: int, dst: int, src_type: str, dst_type: str,
                 delay: float, bandwidth: float) -> None:
        self.add_node(src, src_type)
        self.add_node(dst, dst_type)
        
        link = Link(src, dst, src_type, dst_type, delay, bandwidth)
        self.edges[src].append(link)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> 'Graph':
        """
        build graph from DataFrame
        """
        graph = cls()
        
        for _, row in df.iterrows():
            src = int(row['a_node_id'])
            dst = int(row['z_node_id'])
            delay = float(row['delay(ms)'])
            bandwidth = float(row['bw(GBps)'])
            src_type = row['a_node_type']
            dst_type = row['z_node_type']
            
            graph.add_edge(src, dst, src_type, dst_type, delay, bandwidth)
            
        return graph

    def get_neighbors(self, node_id: int) -> List[Link]:
        """获取节点的所有邻居信息"""
        return self.edges.get(node_id, [])

    def get_node_type(self, node_id: int) -> Optional[str]:
        """获取节点类型"""
        return self.nodes.get(node_id)

    def get_link(self, src: int, dst: int) -> Optional[Link]:
        """获取两个节点之间的链路"""
        for link in self.edges.get(src, []):
            if link.dst_rank == dst:  # 修正: 使用 dst_rank 而不是 dst
                return link
        return None

    def get_all_nodes(self) -> Set[int]:
        """获取所有节点ID"""
        return set(self.nodes.keys())

    def find_all_paths(self, src: int, dst: int, max_paths: int = 5) -> List[List[Link]]:
        """
        search for multiple possible paths between two nodes
        use BFS variant to find multiple different paths
        Args:
            src: source node
            dst: destination node
            max_paths: maximum number of paths
        Returns:
            list of paths, each path is a list of Link objects
        """
        # TODO: 这个算法的问题在于求解的是完全不相交的路径（在当前数据集下只能求出一个路径），
        # 但在路由分配时可能存在部分重叠的路径容量不同的情况，即存在流的分裂。
        paths = []
        queue = deque([(src, [], set())])
        
        while queue and len(paths) < max_paths:
            current, path, visited = queue.popleft()
            
            if current == dst:
                paths.append(path)
                continue
                
            for link in self.edges.get(current, []):
                next_node = link.dst_rank
                if next_node not in visited:
                    new_visited = visited | {next_node}
                    queue.append((next_node, path + [link], new_visited))
                    
        return paths

    def __str__(self) -> str:
        return f"Graph(nodes={len(self.nodes)}, edges={sum(len(e) for e in self.edges.values())})"
