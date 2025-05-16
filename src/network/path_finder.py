from .graph import Graph, Link
import heapq

class PathFinder:
    def __init__(self, graph: Graph):
        self.graph = graph

    def find_path(self, src: int, dst: int) -> list[Link]:
        # 寻找一条最短路
        path: list[Link] = []

        pq = []  # 优先队列，存储 (优先级, 当前节点, 路径)
        heapq.heappush(pq, (0, src, []))
        visited = set()

        while pq:
            _, node, path = heapq.heappop(pq)
            if node in visited:
                continue
            visited.add(node)

            if node == dst:
                return path

            for link in self.graph.edges.get(node, []):
                if link.dst not in visited:
                    heapq.heappush(pq, (link.capacity, link.dst, path + [link]))

        return []
    
    def find_multi_path(self, src: int, dst: int, num_paths: int = 3) -> list[list[Link]]:
        paths: list[list[Link]] = []
        visited = set()
        pq = []
        heapq.heappush(pq, (0, src, []))
        while pq and len(paths) < num_paths:
            _, node, path = heapq.heappop(pq)
            if node in visited:
                continue
            visited.add(node)

            if node == dst:
                paths.append(path)
                continue

            for link in self.graph.edges.get(node, []):
                if link.dst not in visited:
                    # 使用链路容量作为优先级（或其他权重）
                    heapq.heappush(pq, (link.capacity, link.dst, path + [link]))
        return paths

    # def find_multi_path(self, src: int, dst: int, num_paths: int = 3) -> list[list[Link]]:
    #     """
    #     使用Yen算法寻找k条最短路径
        
    #     Args:
    #         src: 源节点
    #         dst: 目标节点
    #         num_paths: 需要寻找的路径数量
            
    #     Returns:
    #         包含多条路径的列表，每条路径是一个Link列表
    #     """
    #     # Yen's Algorithm for k-shortest paths (KSP)
        
    #     # 第一步：找到第一条最短路径 P(1)
    #     shortest_path = self.find_path(src, dst)
    #     if not shortest_path:
    #         return []  # 如果找不到路径，返回空列表
            
    #     # 存储已找到的k条最短路径
    #     k_paths = [shortest_path]
    #     # 存储候选路径
    #     candidates = []
        
    #     # 第二步：依次找到其他的k-1条最短路径
    #     for k in range(1, num_paths):
    #         # 如果已经找不到更多路径，就退出
    #         if k-1 >= len(k_paths):
    #             break
                
    #         # 获取上一条找到的最短路径
    #         prev_path = k_paths[k-1]
            
    #         # 遍历上一条路径中除了终点外的所有节点
    #         for i in range(len(prev_path)):
    #             # 获取偏离节点和到偏离节点的路径
    #             spur_node = prev_path[i].src if i == 0 else prev_path[i-1].dst
    #             root_path = prev_path[:i]  # 从起点到偏离节点的路径
                
    #             # 临时存储需要恢复的边
    #             removed_edges = []
                
    #             # 移除已找到路径中在root_path之后使用的边，避免生成相同的路径
    #             for p in k_paths:
    #                 if i < len(p) and (i == 0 or (i > 0 and self._paths_match(root_path, p[:i]))):
    #                     # 如果当前路径与root_path匹配，移除下一条边
    #                     next_edge = p[i]
    #                     if next_edge in self.graph.edges.get(spur_node, []):
    #                         removed_edges.append(next_edge)
    #                         self.graph.edges[spur_node].remove(next_edge)
                
    #             # 从偏离节点开始寻找到终点的路径
    #             spur_path = self._find_path_from_node(spur_node, dst)
                
    #             # 恢复移除的边
    #             for edge in removed_edges:
    #                 if spur_node in self.graph.edges:
    #                     if edge not in self.graph.edges[spur_node]:
    #                         self.graph.edges[spur_node].append(edge)
    #                 else:
    #                     self.graph.edges[spur_node] = [edge]
                
    #             # 如果找到了偏离路径，组合root_path和spur_path形成候选路径
    #             if spur_path:
    #                 candidate_path = root_path + spur_path
    #                 # 检查这个候选路径是否已经在候选列表中
    #                 if not any(self._paths_equal(candidate_path, p) for p in candidates):
    #                     # 计算路径的总容量（或其他权重）
    #                     path_capacity = sum(link.capacity for link in candidate_path)
    #                     heapq.heappush(candidates, (path_capacity, candidate_path))
            
    #         # 如果没有候选路径，结束算法
    #         if not candidates:
    #             break
                
    #         # 从候选路径中选取最短的一条加入到k_paths中
    #         _, next_path = heapq.heappop(candidates)
    #         k_paths.append(next_path)
            
    #     return k_paths
        
    # def _paths_match(self, path1: list[Link], path2: list[Link]) -> bool:
    #     """检查两个路径是否匹配（路径相等）"""
    #     if len(path1) != len(path2):
    #         return False
    #     return all(self._links_equal(path1[i], path2[i]) for i in range(len(path1)))
        
    # def _paths_equal(self, path1: list[Link], path2: list[Link]) -> bool:
    #     """检查两个路径是否相等"""
    #     if len(path1) != len(path2):
    #         return False
    #     return all(self._links_equal(path1[i], path2[i]) for i in range(len(path1)))
        
    # def _links_equal(self, link1: Link, link2: Link) -> bool:
    #     """检查两个链路是否相等"""
    #     return link1.src == link2.src and link1.dst == link2.dst
        
    # def _find_path_from_node(self, src: int, dst: int) -> list[Link]:
    #     """从指定节点开始寻找到目标节点的最短路径"""
    #     pq = []  # 优先队列，存储 (优先级, 当前节点, 路径)
    #     heapq.heappush(pq, (0, src, []))
    #     visited = set()
        
    #     while pq:
    #         _, node, path = heapq.heappop(pq)
    #         if node in visited:
    #             continue
    #         visited.add(node)
            
    #         if node == dst:
    #             return path
                
    #         for link in self.graph.edges.get(node, []):
    #             if link.dst not in visited:
    #                 heapq.heappush(pq, (link.capacity, link.dst, path + [link]))
                    
    #     return []

