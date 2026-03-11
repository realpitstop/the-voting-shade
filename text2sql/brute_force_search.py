import networkx as nx
import pickle
from constants import GRAPH_PATH
from itertools import permutations

# load graph
with open(GRAPH_PATH, 'rb') as f:
    G = pickle.load(f)

# 
def shortest_path(G, required_nodes, weight=None):
    """
    Method to get the shortest path between graph nodes
    """
    min_dist = float('inf')
    best_path_sequence = None

    for path_sequence in permutations(required_nodes):
        current_dist = 0
        possible_path = True

        for i in range(len(path_sequence) - 1):
            try:
                dist = nx.shortest_path_length(G, path_sequence[i], path_sequence[i + 1], weight=weight)
                current_dist += dist
            except nx.NetworkXNoPath:
                possible_path = False
                break

        if possible_path and current_dist < min_dist:
            min_dist = current_dist
            best_path_sequence = path_sequence
    best_path = []
    best_path_sequence = list(best_path_sequence)
    for i in range(len(best_path_sequence) - 1):
        dist = nx.shortest_path(G, best_path_sequence[i], best_path_sequence[i + 1], weight=weight)
        if len(best_path) >= 2: best_path.pop(-1)
        best_path.extend(dist)
    return best_path


def get_shortest_path(tables, aggs):
    """
    Get the shortest join path between a list of tables in graph, taking into account aggregated metrics
    """
    # Handle empty or single table cases
    if not tables:
        return []
    
    tables = list(tables)
    if len(tables) == 1:
        return [tables]
    
    # get shortest path
    path = shortest_path(G, tables)
    
    # path with foreign keys
    fk_path = [[]]

    # whether an aggregated metric is part of the current joined table (prevent duplication)
    metric = False

    # whether the table has been fanned out (duplicated) and how (many to many, one to many, etc.)
    fanned = None

    # seen tables so that the path doesn't loop around (not necessary in SQL)
    seen = set()
    for i in range(len(path)-1):
        u, v = path[i], path[i+1]
        if v in seen: continue
        if u in aggs: metric = True # aggregated metric part of table
        seen.add(u)

        edge_data = G.get_edge_data(u, v)
        fk = edge_data.get('foreign_key')
        fanout = edge_data.get('fanout')

        # conditions for whether or not to split and use a new table to prevent duplication
        if (fanout == 1 and metric) or (fanned and v in aggs):
            fk_path.append([])
            metric = False
            fanned = False
        else:
            if fanout != 0: fanned = True
        fk_path[-1].append([u, v, fk, fanout])
    return fk_path

def get_path_as_sql(path):
    """
    Get given join path in format [[table1, table2, foreignkey]] in SQL format
    """
    pathString = ""
    for i in range(len(path)):
        if i == 0:
            pathString += "\nFROM\n\t" + path[i][0]
            if len(path[i]) == 1: break
        pathString += f"\nINNER JOIN\n\t{path[i][1]} ON {path[i][0]}.{path[i][2]} = {path[i][1]}.{path[i][2]}"
    return pathString