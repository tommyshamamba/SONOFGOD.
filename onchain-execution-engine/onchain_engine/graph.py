"""Price-graph cycle search. Pure functions: no RPC, no globals.

graph[u][v] is the effective exchange rate u->v (output units per input unit,
already net of the configured slippage haircut). A cycle is profitable in this
model when the product of its rates exceeds 1, i.e. the sum of -log(rate) is
negative. Note the model limitation documented in README.md: rates are quoted
at one fixed size, so real price impact is not captured.
"""
import heapq
import math
from decimal import Decimal
from typing import Dict, Iterable, List, Set, Tuple

Graph = Dict[str, Dict[str, Decimal]]


def cycle_rate(graph: Graph, cycle: List[str]) -> float:
    """Product of edge rates along a closed cycle (0 if an edge is missing)."""
    prod = 1.0
    for u, v in zip(cycle, cycle[1:]):
        r = graph.get(u, {}).get(v)
        if not r or r <= 0:
            return 0.0
        prod *= float(r)
    return prod


def _canonical(cycle: List[str]) -> Tuple[str, ...]:
    core = cycle[:-1]
    m = min(range(len(core)), key=lambda i: core[i])
    return tuple(core[m:] + core[:m])


def enumerate_cycles(graph: Graph, max_hops: int, blacklist: Iterable[str] = ()) -> List[List[str]]:
    """All simple cycles with 3..max_hops+1 nodes, by DFS. Exponential: small graphs only."""
    banned = set(blacklist)
    out: List[List[str]] = []
    seen: Set[Tuple[str, ...]] = set()

    def dfs(start: str, node: str, path: List[str]) -> None:
        if len(path) > max_hops:
            return
        for nxt in graph.get(node, {}):
            if nxt == start and len(path) >= 3:
                c = path + [start]
                key = _canonical(c)
                if key not in seen:
                    seen.add(key)
                    out.append(c)
            elif nxt not in path and nxt not in banned:
                dfs(start, nxt, path + [nxt])

    for n in graph:
        if n not in banned:
            dfs(n, n, [n])
    return out


def bounded_simple_cycles(graph: Graph, max_hops: int, blacklist: Iterable[str] = ()) -> List[List[str]]:
    """Johnson-style blocking search with a depth cap.

    The cap makes it fast but NOT exhaustive (Johnson's unblocking invariant
    does not hold under a depth limit), so it may miss cycles that
    enumerate_cycles finds. Results are deduplicated by rotation.
    """
    banned = set(blacklist)
    vertices = [v for v in graph if v not in banned]
    cycles: List[List[str]] = []

    for s in vertices:
        blocked: Set[str] = set()
        B: Dict[str, Set[str]] = {v: set() for v in graph}
        stack: List[str] = []

        def unblock(u: str) -> None:
            if u in blocked:
                blocked.discard(u)
                for w in list(B.get(u, ())):
                    B[u].discard(w)
                    unblock(w)

        def circuit(v: str, depth: int) -> bool:
            found = False
            blocked.add(v)
            stack.append(v)
            for w in graph.get(v, {}):
                if w in banned or depth + 1 > max_hops:
                    continue
                if w == s and len(stack) >= 2:
                    cycles.append(stack[:] + [s])
                    found = True
                elif w not in blocked:
                    if circuit(w, depth + 1):
                        found = True
            if found:
                unblock(v)
            else:
                for w in graph.get(v, {}):
                    B.setdefault(w, set()).add(v)
            stack.pop()
            return found

        circuit(s, 0)

    uniq, seen = [], set()
    for c in cycles:
        if len(c) < 4:  # need at least 3 distinct tokens
            continue
        key = _canonical(c)
        if key not in seen:
            seen.add(key)
            uniq.append(c)
    return uniq


def bellman_ford_arbitrage(graph: Graph) -> List[List[str]]:
    """Negative cycles under w = -log(rate), returned closed (first == last).

    Fixes vs the original script: cycles are closed (the old version returned
    open paths, which the caller's `c[0] == c[-1]` check silently discarded, so
    Bellman-Ford never contributed), predecessor walks cannot index -1, and
    duplicates are removed.
    """
    nodes = list(graph.keys() | {v for u in graph for v in graph[u]})
    idx = {n: i for i, n in enumerate(nodes)}
    edges = [(idx[u], idx[v], -math.log(float(r))) for u in graph for v, r in graph[u].items() if r and r > 0]
    n = len(nodes)
    dist = [0.0] * n  # virtual source connected to every node
    prev = [-1] * n
    for _ in range(n - 1):
        changed = False
        for u, v, w in edges:
            if dist[u] + w < dist[v] - 1e-15:
                dist[v] = dist[u] + w
                prev[v] = u
                changed = True
        if not changed:
            return []
    out: List[List[str]] = []
    seen: Set[Tuple[str, ...]] = set()
    for u, v, w in edges:
        if dist[u] + w < dist[v] - 1e-15:
            prev[v] = u
            x = v
            for _ in range(n):  # step back n times to land inside the cycle
                if prev[x] == -1:
                    break
                x = prev[x]
            if prev[x] == -1:
                continue
            cyc = [x]
            cur = prev[x]
            while cur != x and cur != -1 and len(cyc) <= n:
                cyc.append(cur)
                cur = prev[cur]
            if cur != x:
                continue
            path = [nodes[i] for i in reversed(cyc)]
            closed = path + [path[0]]
            key = _canonical(closed)
            if key not in seen and cycle_rate(graph, closed) > 1.0:
                seen.add(key)
                out.append(closed)
    return out


def dijkstra_best_path(graph: Graph, src: str) -> Dict[str, Tuple[float, List[str]]]:
    """Best-rate simple paths from src. Valid only while all rates <= 1
    (non-negative weights); used for local refinement, not cycle detection."""
    dist: Dict[str, float] = {src: 0.0}
    path: Dict[str, List[str]] = {src: [src]}
    pq = [(0.0, src)]
    while pq:
        d, u = heapq.heappop(pq)
        if d > dist.get(u, math.inf):
            continue
        for v, r in graph.get(u, {}).items():
            if not r or r <= 0:
                continue
            nd = d + max(0.0, -math.log(float(r)))
            if nd < dist.get(v, math.inf):
                dist[v] = nd
                path[v] = path[u] + [v]
                heapq.heappush(pq, (nd, v))
    return {k: (math.exp(-d), path[k]) for k, d in dist.items()}


def refine_cycle_with_dijkstra(graph, cycle, max_hops, min_improve: float = 0.003, max_extra_hops_per_edge: int = 2) -> List[str]:
    if not cycle or len(cycle) < 3:
        return cycle
    start = cycle[0]
    refined: List[str] = [start]
    current_hops = 0
    for i in range(len(cycle) - 1):
        u, v = cycle[i], cycle[i+1]
        direct = graph.get(u, {}).get(v)
        if direct is None or direct <= 0:
            if refined[-1] != u:
                refined.append(u)
            refined.append(v)
            current_hops += 1
            continue
        best_from_u = dijkstra_best_path(graph, u)
        best_uv = best_from_u.get(v)
        if not best_uv:
            if refined[-1] != u:
                refined.append(u)
            refined.append(v)
            current_hops += 1
            continue
        rate, path = best_uv
        extra_hops = max(0, len(path) - 2)
        if extra_hops > max_extra_hops_per_edge:
            path = [u, v]
            extra_hops = 0
        if rate <= float(direct) * (1.0 + min_improve) or (current_hops + 1 + extra_hops) > max_hops:
            path = [u, v]
            extra_hops = 0
        if refined[-1] != u:
            refined.append(u)
        for node in path[1:]:
            refined.append(node)
        current_hops += 1 + extra_hops
        if current_hops > max_hops:
            if refined[-1] != start:
                refined[-1] = start
            break
    if refined[-1] != start:
        refined.append(start)
    if len(refined) - 1 > max_hops:
        return cycle
    return refined
