import json

import duckdb
import pandas as pd
import numpy as np

from make_sql import build_sql_from_request
from constants import SCHEMA_PATH
from QueryBuilder import QueryBuilder

CLEAN_PATH = "../data/clean/"
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    tables = set(i["table"] for i in json.load(f))

for i in tables:
    globals()[i] = pd.read_csv(CLEAN_PATH + f'{i}.csv')

request = QueryBuilder(limit=None)
(
    request
    .add_filter("industry", "oil and gas")
    .add_filter("subtopic", "oil and gas")
    .add_filter("role", "sponsor")
    .add_filter("party", "Republican")
    .add_filter("chamber", "senate")
    .add_metric("sponsor", "COUNT")
    .add_metric("money taken", "SUM")
    .add_group("person name")
    .set_rank("sponsor", "DESC", "COUNT")
)

query = build_sql_from_request(request, real=False)
pd.set_option('display.max_columns', None)

pd.set_option('display.max_rows', None)

pd.set_option('display.width', 1000)

print("\n\n" + query)

result_df = duckdb.query(query).to_df()

print(result_df)
print(np.corrcoef(result_df.iloc[:, -2].to_numpy(), result_df.iloc[:, -1].to_numpy())[0, 1])