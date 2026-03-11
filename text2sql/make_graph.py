import networkx as nx
import pickle
import json
from constants import SCHEMA_PATH, GRAPH_PATH

G = nx.DiGraph()
with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
    schema = json.load(f)

# construct graph
for i in schema:
    # add table if not exists
    G.add_node(i["table"])

    # only allow foreign keys
    if i.get("type", None) != "primarykey" and i.get("type", None) is not None:
        # add both edges. fanout -1 means many to 1, 1 means 1 to many, 0 means 1 to 1
        # from table to foreign key table
        G.add_edge(i["table"], i["type"], foreign_key=i["column"], fanout=-i.get("fanout", 0))
        # from foreign key table to table
        G.add_edge(i["type"], i["table"], foreign_key=i["column"], fanout=i.get("fanout", 0))

# save graph
with open(GRAPH_PATH, 'wb') as f:
    pickle.dump(G, f)