from collections import defaultdict, deque
from .graph import Graph, Link
from .demand import Demand
import heapq

class PathFinder:
    def __init__(self, graph: Graph):
        self.graph = graph
        self.link_allocated_bw: dict[(int, int), float] = {}

    def find_all_paths(self, src: int, dst: int, max_paths: int = 5) -> list[list[Link]]:
        return self.graph.find_all_paths(src, dst, max_paths)

    def find_path(self, src: int, dst: int) -> list[Link]:
        """
        Dijkstra: search for the shortest path between two nodes
        use link delay as weight to find the path with minimum total delay
        return:
            List[Link]: list of nodes in the path
        """
        distances = {node: float('infinity') for node in self.graph.get_all_nodes()}
        distances[src] = 0
        # priority queue element format: (total delay, current node, path so far)
        pq = [(0, src, [])]
        visited = set()
        
        while pq:
            (dist, current, path) = heapq.heappop(pq)
            if current in visited:
                continue
            visited.add(current)
            if current == dst:
                return path
                
            # traverse neighbors
            for link in self.graph.get_neighbors(current):
                if link.dst_rank not in visited:
                    new_dist = dist + link.delay
                    if new_dist < distances[link.dst_rank]:
                        distances[link.dst_rank] = new_dist
                        heapq.heappush(pq, (new_dist, link.dst_rank, path + [link]))
        
        return []

    def allocate_demand_bandwidth(self, demand: Demand) -> list[tuple[list[Link], float]]:
        result: list[tuple[list[Link], float]] = []
        demand_remaining_bw = demand.bandwidth
        
        all_paths = self.find_all_paths(demand.src_rank, demand.dst_rank)
        # print(f"Found {len(all_paths)} paths between {demand.src_rank} and {demand.dst_rank}")
        
        for path in all_paths:
            if demand_remaining_bw <= 0:
                break
                
            # calculate available bandwidth on this path
            path_available_bw = float('inf')
            for link in path:
                link_pair = (link.src_rank, link.dst_rank)
                if link_pair not in self.link_allocated_bw:
                    self.link_allocated_bw[link_pair] = 0
                link_available_bw = link.bandwidth - self.link_allocated_bw[link_pair]
                path_available_bw = min(path_available_bw, link_available_bw)
            
            if path_available_bw <= 0:
                continue
                
            allocated_bw = min(demand_remaining_bw, path_available_bw)
            for link in path:
                link_pair = (link.src_rank, link.dst_rank)
                self.link_allocated_bw[link_pair] += allocated_bw
                
            result.append((path, allocated_bw))
            demand_remaining_bw -= allocated_bw
        
        if demand_remaining_bw > 0:
            # failed allocating, restore allocated bandwidth
            for path, allocated_bw in result:
                for link in path:
                    link_pair = (link.src_rank, link.dst_rank)
                    self.link_allocated_bw[link_pair] -= allocated_bw
            return []
            
        return result
