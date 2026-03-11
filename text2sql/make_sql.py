import json
from collections import defaultdict
from datetime import datetime

from brute_force_search import get_shortest_path, get_path_as_sql
from match_query import getFaissMatch
from constants import SCHEMA_PATH, STANCES_PATH, SIC_MEANINGS
from pypika import Table, Criterion, functions as fn
from pypika.terms import LiteralValue

# Only operations you can use on all values
ALLOWED_OPS = {
    "str": {"="},
    "int": {"=", ">", "<", ">=", "<="},
    "date": {"=", ">", "<", ">=", "<="}
}

# get the type of the value
def infer_type(value):
    if isinstance(value, int):
        return "int"
    if isinstance(value, datetime):
        return "date"
    return "str"


with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    schema = json.load(f)

# get list of columns, topics, subtopics, industries
COLUMN_LIST = {
    item["column"] + " " + item["description"]: {"column": item["column"], "table": item["table"]}
    for item in schema if item.get("type", None) is None
}
COLUMNS = list(COLUMN_LIST.keys())

TOPIC_LIST = {}
SUBTOPIC_LIST = {}
with open(STANCES_PATH, "r", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        parts = record["text"].split(" – ")
        if len(parts) < 2:
            continue
        title, desc = parts
        if record["code"][-2:] == "00":
            TOPIC_LIST[record["text"]] = title
        else:
            SUBTOPIC_LIST[record["text"]] = title
TOPICS = list(TOPIC_LIST.keys())
SUBTOPICS = list(SUBTOPIC_LIST.keys())
with open(SIC_MEANINGS, "r", encoding="utf-8") as f:
    INDUSTRIES = list(set(json.load(f).values()))
print(INDUSTRIES)

TOPICS = list(TOPIC_LIST.keys())

# get the GROUP BY and HAVING portion of the SQL command
def get_group_string(group_by):
    group_col = set()
    group_string = ""
    if group_by:
        for i in group_by:
            col = f"{i['table']}.{i['column']}"
            if i.get('value'):
                col = f"{i['agg']}({col})" if i.get('agg') else col
                group_col.add(f"HAVING\n\t{col} {i['op']} {i['value']}")
            else:
                group_col.add(col)
        group_string = f"\nGROUP BY\n\t{',\n\t'.join([i for i in group_col if not i.startswith('HAVING')])}"
    return group_string, group_col

# get the conditions from a list of criterion
def get_where_string(cols, real, params):
    criteria = []
    having = []
    for i in range(len(cols)):
        concat, col, table, op, value, agg = cols[i]

        term = make_condition(col, table, op, value, agg, real, params)

        if concat == "OR" and criteria and not agg:
            criteria[-1] = criteria[-1] | term
        elif concat == "OR" and having and agg:
            having[-1] = having[-1] | term
        elif not agg:
            criteria.append(term)
        elif agg:
            having.append(term)

    return params, criteria, having

# make conditions (abc == "abc")
def make_condition(col, table, op, value, agg, real, params):
    addedVal = value
    if isinstance(value, datetime):
        addedVal = value.strftime('%Y-%m-%d')
    elif col == "topic":
        addedVal = TOPIC_LIST[getFaissMatch(value, TOPICS)]
    elif col == "subtopic":
        addedVal = SUBTOPIC_LIST[getFaissMatch(value, SUBTOPICS)]
    elif col == "industry":
        addedVal = getFaissMatch(value, INDUSTRIES)

    if real: params.append(addedVal)

    if real:
        val_placeholder = LiteralValue("?")
    else:
        if isinstance(addedVal, str):
            val_placeholder = LiteralValue(f"'{addedVal}'")
        else:
            val_placeholder = addedVal

    column_obj = Table(table).field(col)

    if agg:
        # Map string aggregate names to Pypika functions
        agg_map = {
            "SUM": fn.Sum,
            "AVG": fn.Avg,
            "COUNT": fn.Count,
            "MAX": fn.Max,
            "MIN": fn.Min
        }
        # Apply the function (e.g., fn.Sum(column_obj))
        column_term = agg_map[agg.upper()](column_obj)
        # Give it an alias to avoid name collisions
        column_term = column_term.as_(f"{agg}_{col}")
    else:
        column_term = column_obj

    if op == "=":
        term = (column_term == val_placeholder)
    elif op == ">":
        term = (column_term > val_placeholder)
    elif op == "<":
        term = (column_term < val_placeholder)
    elif op == ">=":
        term = (column_term >= val_placeholder)
    elif op == "<=":
        term = (column_term <= val_placeholder)

    return term


def build_sql_from_request(request, real=True) -> tuple[str, list[str]] | str:
    """
    {
        filters: [{"concat": ..., "column": ..., "op": ..., "value": ...}, ...],

        metric: [{"field": ..., "agg": ...}, ...],

        rank: {"column": ..., "order": ...},

        group_by: [{"column": ..., "order": ...},...],

        limit: int
    }
    """
    # get request attributes
    filters, metric, rank, limit, group_by = request.filters, request.metrics, request.rank, request.limit, request.group_by

    # parameter values to replace the ?
    params = []

    # tables with aggregated values
    aggs = set()

    # matching tables with all their respective request attributes
    tablematch = defaultdict(lambda: defaultdict(list))

    # match metric columns with their closest semantic match and add it to table match and aggs
    metric = [{**m, **COLUMN_LIST[getFaissMatch(m["field"], COLUMNS)]} for m in metric]
    for i in metric:
        tablematch[i["table"]]["metric"].append(i)
        if i["agg"]:
            aggs.add(i['table'])

    # match rank column with its closest semantic match and add it to table match and aggs
    if rank:
        rank = {**rank, **COLUMN_LIST[getFaissMatch(rank["column"], COLUMNS)]}
        tablematch[rank["table"]]["rank"].append(rank)
        if rank['agg']:
            aggs.add(rank['table'])

    # match group by columns with their closest semantic match and add it to table match
    if group_by:
        group_by = [{**g, **COLUMN_LIST[getFaissMatch(g['field'], COLUMNS)]} for g in group_by]
        for i in group_by:
            tablematch[i["table"]]["group_by"].append(i)

    # match filter columns with their closest semantic matches
    filters = [{**f, **COLUMN_LIST[getFaissMatch(f["column"], COLUMNS)]} for f in filters]

    cols = []

    # make a list of [concatenator, column, table, operation, value, aggregation] for all filters
    for f in filters:
        concat = f["concat"]
        col = f["column"]
        table = f["table"]
        op = f["op"]
        value = f["value"]
        agg = f["agg"]

        if agg:
            aggs.add(table)

        tablematch[table]["filter"].append([concat, col, table, op, value, agg])

        if col == "introduced_date":
            value = datetime.strptime(value, "%m/%d/%Y")

        cols.append([concat, col, table, op, value, agg])

    # get list of tables
    tables = set(tablematch.keys())
    # get the shortest path
    path = get_shortest_path(tables, set(aggs))

    # prepare CTE if necessary
    CTE_string = ""

    # dict to map tables to their CTE
    table_map = {}
    if len(path) == 1:
        # no CTE required
        split = False
        path = path[0]
    else:
        split = True
        CTEs = []
        new_path = []
        # generate names for each CTE
        names = [i[0][0][:3].capitalize() + i[-1][1][:3].capitalize() for i in path]
        for i in range(len(path)):
            subpath, cte_name = path[i], names[i]
            if i < (len(path) - 1):
                # add to the new join path
                new_path.append([names[i], names[i+1], subpath[-1][2]])
            
            # create start of table "TABLE NAME AS ("
            table_string = "\n" + cte_name + " AS ("
            subTables = set()
            for p in subpath:
                # add all the tables in the subpath
                subTables.add(p[0])
                subTables.add(p[1])
            
            # dict to group every query attribute with all it's instances- removing tables
            final = defaultdict(list)
            all = []
            for table in subTables:
                table_map[table] = cte_name
                for k, v in tablematch[table].items():
                    for ite in v:
                        if isinstance(ite, list): # if it's a filter string
                            all.append([ite[2], ite[1], ite[5]])
                        if isinstance(ite, dict):
                            print(ite)
                            all.append([ite['table'], ite['column'], ite['agg']])
                    final[k].extend(v)

            # add the agged metrics with alias as opped_name
            select_string = "\nSELECT\n\t" + ",\n\t".join([f"{a[2]}({a[0]}.{a[1]}) AS opped_{a[1]}" for a in all if a[2]])
            # get the join path for the CTE
            join_string = get_path_as_sql(subpath)
            # get the filter where + having strings for the CTE
            params, criteria, having = get_where_string(final['filter'], real, params)

            where_string = ""
            if criteria:
                final_criterion = Criterion.all(criteria)
                where_string += f"\nWHERE {final_criterion.get_sql(quote_char=None, with_namespace=True).replace(" AND ", "\n\tAND ").replace(" OR ", "\n\tOR ")}"

            having_string = ""
            if having:
                final_having = Criterion.all(having)
                having_string += f"\nHAVING {final_having.get_sql(quote_char=None, with_namespace=True).replace(" AND ", "\n\tAND ").replace(" OR ", "\n\tOR ")}"
            
            # get group by including what was orignally intended and all the non-aggregated things desired
            group_string, group_col = get_group_string(final['group_by'] + [{"table": subpath[-1][1], "column": subpath[-1][2]} for _ in [1] if i < len(path) - 1] + [{"table": subpath[0][1], "column": subpath[0][2]} for _ in [1] if i > 0])
            
            # add the group_by things into the select statement
            select_string += (",\n\t" if group_col else "\n\t") + ",\n\t".join(group_col) + (f",\n\t{subpath[-1][1]}.{subpath[-1][2]}" if i < len(path) - 1 else "") + (f",\n\t{subpath[0][1]}.{subpath[0][2]}" if i > 0 else "")
            # assemble final CTE
            cte_string = table_string + select_string.replace('\n', '\n\t') + join_string.replace('\n', '\n\t') + where_string.replace('\n', '\n\t') + group_string.replace('\n', '\n\t') + having_string.replace('\n', '\n\t') + "\n)"
            CTEs.append(cte_string)
            if rank: rank['table'] = names[i] if rank['table'] in subTables else rank['table']
        CTE_string = "WITH" + ",\n".join(CTEs) + "\n\n"
        path = new_path

    # get join path of path (updated if CTE are included)
    join_string = get_path_as_sql(path)
    final_cols = []
    for i in range(len(cols)):
        col = cols[i]
        if table_map.get(col[2], None) is None: final_cols.append(col)

    params, criteria, having = get_where_string(final_cols, real, params)
    
    # get where (filters) & having (filters w aggregations)
    where_string = ""
    if criteria:
        final_criterion = Criterion.all(criteria)
        where_string += f"\nWHERE\n\t{final_criterion.get_sql(quote_char=None, with_namespace=True).replace(" AND ", "\n\tAND ").replace(" OR ", "\n\tOR ")}"

    having_string = ""
    if having:
        final_having = Criterion.all(having)
        having_string += f"\nHAVING {final_having.get_sql(quote_char=None, with_namespace=True).replace(" AND ", "\n\tAND ").replace(" OR ", "\n\tOR ")}"

    # get ordering string
    order_string = ""
    if rank:
        rank['table'] = table_map.get(rank['table'], rank['table'])
        if rank["agg"] is None:
            order_string = f"\nORDER BY {rank['table']}.{rank['column']} {rank['order']}"
        else:
            order_string = f"\nORDER BY {rank['agg']}({rank['table']}.{rank['column']}) {rank['order']}" if not split else f"\nORDER BY {rank['table']}.opped_{rank['column']} {rank['order']}"

    # get select string that has the metrics
    select_string = "SELECT "
    for m in metric:
        m['table'] = table_map.get(m['table'], m['table'])
        if m.get("agg"):
            if not split: select_string += f"{m['agg']}({m['table']}.{m['column']}),\n\t"
            if split: select_string += f"{m['table']}.opped_{m['column']},\n\t"
    
    # if there's nothing just get everything
    if select_string == "SELECT ":
        select_string += f"DISTINCT *"
    else:
        select_string = select_string[:-2]

    # get the group by string for whatever is supposed to be grouped
    group_string = "", []
    if group_by:
        group_string, group_col = get_group_string([gp for gp in group_by if table_map.get(gp['table'], None) is None])

    group_by = [{**gp, 'table': table_map.get(gp['table'], gp['table'])} for gp in group_by]

    # assemble the true final select string
    select_cols = [f"{group['table']}.{group['column']}" for group in group_by]
    select_string = f"SELECT DISTINCT\n\t{',\n\t'.join(select_cols) + ', ' if select_cols else ""}"

    select_string += ", ".join([f"{m['agg']}({m['table']}.{m['column']})" if m.get('agg') and not split else f"{m['table']}.opped_{m['column']}" if m.get('agg') and split else f"{m['table']}.{m['column']}" for m in metric])

    # add limit
    if limit:
        order_string += f"\nLIMIT {limit}"

    # assemble final sql query
    sql = CTE_string + select_string + join_string + where_string + group_string + having_string + order_string + ";"

    # return with params if it's a real request that needs to be protected
    return sql if not real else (sql, params)
