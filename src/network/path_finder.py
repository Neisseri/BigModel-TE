from .graph import Graph, Link
import heapq

class PathFinder:
    def __init__(self, graph: Graph):
        self.graph = graph

    def find_path(self, src: int, dst: int) -> list[Link]:
        # 为简化实现，仅寻找一条最短路（即每个负载分配一条流）
        path: list[Link] = []

        pq = []  # 优先队列，存储 (当前节点, 路径)
        heapq.heappush(pq, (src, []))
        visited = set()

        while pq:
            node, path = heapq.heappop(pq)
            visited.add(node)

            if node == dst:
                return path  # 找到目标节点，返回路径

            for link in self.graph.edges.get(node, []):
                if link.dst not in visited:
                    heapq.heappush(pq, (link.dst, path + [link]))

        return []

