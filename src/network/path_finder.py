from .graph import Graph, Link
import heapq

class PathFinder:
    def __init__(self, graph: Graph):
        self.graph = graph

    def find_path(self, src: int, dst: int) -> list[Link]:
        # 为简化实现，仅寻找一条最短路（即每个负载分配一条流）
        path: list[Link] = []

        pq = []  # 优先队列，存储 (优先级, 当前节点, 路径)
        heapq.heappush(pq, (0, src, []))  # 优先级初始化为 0
        visited = set()

        while pq:
            _, node, path = heapq.heappop(pq)  # 解包优先级、当前节点和路径
            if node in visited:
                continue
            visited.add(node)

            if node == dst:
                return path  # 找到目标节点，返回路径

            for link in self.graph.edges.get(node, []):
                if link.dst not in visited:
                    # 使用链路容量作为优先级（或其他权重）
                    heapq.heappush(pq, (link.capacity, link.dst, path + [link]))

        return []

