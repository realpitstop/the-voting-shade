from pathlib import Path
import os

BASE_DIR = Path(os.path.abspath(__file__)).parent.parent.parent

SCHEMA_PATH = os.path.join(BASE_DIR, "/text2sql/schema.json")
GRAPH_PATH = os.path.join(BASE_DIR, "/text2sql/graph.pickle")
STANCES_PATH = os.path.join(BASE_DIR, "/data/clean/stances.jsonl")
SIC_MEANINGS = os.path.join(BASE_DIR, "/data/clean/sic_meaning.json")