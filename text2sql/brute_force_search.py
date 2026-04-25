import pickle
import networkx as nx
from constants import GRAPH_PATH

with open(GRAPH_PATH, 'rb') as f:
    G = pickle.load(f)


def _greedy_path_sequence(G, required_nodes: list, weight=None) -> list:
    """
    Greedy nearest-neighbor ordering of required_nodes.
    """
    unvisited = set(required_nodes)
    current = required_nodes[0]
    sequence = [current]
    unvisited.remove(current)

    while unvisited:
        nearest = min(
            unvisited,
            key=lambda n: nx.shortest_path_length(G, current, n, weight=weight)
        )
        sequence.append(nearest)
        unvisited.remove(nearest)
        current = nearest

    return sequence


def _expand_sequence_to_node_path(G, sequence: list, weight=None) -> list:
    """
    Given an ordered list of required nodes, expand each consecutive pair
    into the full intermediate node path through the graph.
    """
    full_path = []
    for i in range(len(sequence) - 1):
        segment = nx.shortest_path(G, sequence[i], sequence[i + 1], weight=weight)
        if full_path:
            full_path.pop()  # remove duplicate junction node
        full_path.extend(segment)
    return full_path


def get_shortest_path(tables: set, aggs: set) -> list:
    """
    Get the shortest join path between a list of tables in the graph,
    splitting into separate segments where aggregation fanout requires a CTE.

    Returns a list of path segments, where each segment is a list of
    [from_table, to_table, foreign_key, fanout] edges.
    A single-segment result means no CTE is needed.
    """
    if not tables:
        return []

    tables = list(tables)

    if len(tables) == 1:
        return [[tables]]

    sequence = _greedy_path_sequence(G, tables)
    node_path = _expand_sequence_to_node_path(G, sequence)

    # Walk the node path and split into segments where fan-out would cause row duplication.
    fk_path = [[]]
    has_agg_metric = False
    fanned = False
    seen = set()

    for i in range(len(node_path) - 1):
        u, v = node_path[i], node_path[i + 1]
        if v in seen: continue
        if u in aggs: has_agg_metric = True
        seen.add(u)

        edge_data = G.get_edge_data(u, v)
        fk = edge_data.get('foreign_key')
        fanout = edge_data.get('fanout')

        # Split into a new CTE segment when fan-out would duplicate agg rows
        if (fanout == 1 and has_agg_metric) or (fanned and v in aggs):
            fk_path.append([])
            has_agg_metric = False
            fanned = False
        else:
            if fanout != 0: fanned = True
        fk_path[-1].append([u, v, fk, fanout])
    return fk_path


def get_path_as_sql(path: list) -> str:
    """
    Convert a path in [[from_table, to_table, foreign_key, ...], ...] format
    into a FROM / INNER JOIN SQL string.
    """
    if not path:
        return ""

    sql = f"\nFROM\n\t{path[0][0]}"

    # single-table path: path[0] is just [table_name]
    if len(path[0]) == 1:
        return sql

    for edge in path:
        from_table, to_table, fk = edge[0], edge[1], edge[2]
        sql += f"\nINNER JOIN\n\t{to_table} ON {from_table}.{fk} = {to_table}.{fk}"

    return sql