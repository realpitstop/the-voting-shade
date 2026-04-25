import json

import duckdb
import pandas as pd
import numpy as np

from make_sql import build_sql_from_request
from constants import SCHEMA_PATH
from QueryBuilder import QueryBuilder

# file paths
CLEAN_PATH = "./data/clean/"

# get all tables
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    tables = set(i["table"] for i in json.load(f))

# add tables to global variables dynamically (CAN BE DANGEROUS OR DIFFICULT TO USE)
for i in tables:
    globals()[i] = pd.read_csv(CLEAN_PATH + f'{i}.csv')

# Build query (example given)
request = QueryBuilder(limit=250)
(
    request
    .add_filter("election cycle", 2026)
    .add_metric("money taken", "SUM")
    .add_metric("member count")
    .add_group("committee name")
    .set_rank("money taken", "DESC", "SUM")
)

# Build sql from query
query, params = build_sql_from_request(request, real=True)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

# print SQL query
print("\n\n" + query)

# get output
result_df = duckdb.sql(query, params=params).to_df()

print(result_df)