import networkx as nx
import pickle
import json
from constants import SCHEMA_PATH, GRAPH_PATH

G = nx.DiGraph()
with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
    schema = json.load(f)

for i in schema:
    G.add_node(i["table"])
    if i.get("type", None) != "primarykey" and i.get("type", None) is not None:
        G.add_edge(i["table"], i["type"], foreign_key=i["column"], fanout=-i.get("fanout", 0))
        G.add_edge(i["type"], i["table"], foreign_key=i["column"], fanout=i.get("fanout", 0))

with open(GRAPH_PATH, 'wb') as f:
    pickle.dump(G, f)