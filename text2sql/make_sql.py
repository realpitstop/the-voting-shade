import json
from collections import defaultdict
from datetime import datetime

from brute_force_search import get_shortest_path, get_path_as_sql
from match_query import Matcher
from constants import SCHEMA_PATH, STANCES_PATH, SIC_MEANINGS
from pypika import Table, Criterion, functions as fn
from pypika.terms import LiteralValue

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    _schema = json.load(f)

# Maps "column_name description_text" to {column, table}
# Only includes non key columns
COLUMN_LIST: dict[str, dict] = {
    item["column"] + " " + item["description"]: {"column": item["column"], "table": item["table"]}
    for item in _schema if item.get("type") is None
}
COLUMNS: list[str] = list(COLUMN_LIST.keys())

TOPIC_LIST: dict[str, str] = {}
SUBTOPIC_LIST: dict[str, str] = {}
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
 
with open(SIC_MEANINGS, "r", encoding="utf-8") as f:
    INDUSTRIES: list[str] = list(set(json.load(f).values()))

_column_matcher   = Matcher(COLUMNS)
_topic_matcher    = Matcher(list(TOPIC_LIST.keys()))
_subtopic_matcher = Matcher(list(SUBTOPIC_LIST.keys()))
_industry_matcher = Matcher(INDUSTRIES)

_AGG_MAP = {
    "SUM":   fn.Sum,
    "AVG":   fn.Avg,
    "COUNT": fn.Count,
    "MAX":   fn.Max,
    "MIN":   fn.Min,
}

# helpers
def _resolve_column(field: str) -> dict:
    """Semantic-match a field name to {column, table}"""
    return COLUMN_LIST[_column_matcher.match(field)]


def _resolve_filter_value(col: str, value):
    """
    For special columns (topic/subtopic/industry/date), translate the
    raw user value into the DB value. Only for special columns.
    """
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')
    if col == "topic":
        return TOPIC_LIST[_topic_matcher.match(value)]
    if col == "subtopic":
        return SUBTOPIC_LIST[_subtopic_matcher.match(value)]
    if col == "industry":
        return _industry_matcher.match(value)
    if col == "introduced_date" and isinstance(value, str):
        return datetime.strptime(value, "%m/%d/%Y").strftime('%Y-%m-%d')
    return value


def _make_condition(col: str, table: str, op: str, value, agg: str | None,
                            real: bool, params: list):
    """
    Build a single criterion for one filter clause.
    Appends to `params` when real=True (real implementation, not testing).
    """
    resolved = _resolve_filter_value(col, value)

    if real:
        params.append(resolved)
        val_term = LiteralValue("?")
    else:
        val_term = LiteralValue(f"'{resolved}'") if isinstance(resolved, str) else resolved

    column_obj = Table(table).field(col)

    if agg:
        agg_fn = _AGG_MAP[agg.upper()]
        column_term = agg_fn(column_obj).as_(f"{agg}_{col}")
    else:
        column_term = column_obj

    ops = {"=": column_term.__eq__, ">": column_term.__gt__,
           "<": column_term.__lt__, ">=": column_term.__ge__, "<=": column_term.__le__}
    return ops[op](val_term)


def _build_where_and_having(filter_rows: list, real: bool, params: list):
    """
    Turn a list of [concat, col, table, op, value, agg] rows into separate
    criterion lists for WHERE and HAVING.
    """
    criteria = []
    having = []

    for concat, col, table, op, value, agg in filter_rows:
        term = _make_condition(col, table, op, value, agg, real, params)
        target = having if agg else criteria

        if concat == "OR" and target:
            target[-1] = target[-1] | term
        else:
            target.append(term)

    return criteria, having


def _criterion_to_sql(criterion_list: list) -> str:
    """
    Converts criterion into string SQL equivalent.
    """
    return (
        Criterion.all(criterion_list)
        .get_sql(quote_char=None, with_namespace=True)
        .replace(" AND ", "\n\tAND ")
        .replace(" OR ",  "\n\tOR ")
    )


def _build_group_by_sql(group_by_items: list) -> tuple[str, set]:
    """
    Returns (GROUP BY sql string, set of group expressions).
    Items with a value produce HAVING clauses; others produce GROUP BY columns.
    """
    group_cols = set()
    having_clauses = set()

    for item in group_by_items:
        col_expr = f"{item['table']}.{item['column']}"
        if item.get('value'):
            agg_expr = f"{item['agg']}({col_expr})" if item.get('agg') else col_expr
            having_clauses.add(f"HAVING\n\t{agg_expr} {item['op']} {item['value']}")
        else:
            group_cols.add(col_expr)

    group_string = ""
    if group_cols:
        group_string = f"\nGROUP BY\n\t{',\n\t'.join(group_cols)}"

    return group_string, group_cols | having_clauses


# CTE builder

def _build_cte(
    subpath: list,
    cte_name: str,
    cte_index: int,
    total_ctes: int,
    tablematch: dict,
    real: bool,
    params: list,
) -> tuple[str, dict]:
    """
    Build one CTE block and return (cte_sql_string, {original_table: cte_name} map).
    """
    # Collect all original tables covered by this subpath
    sub_tables = set()
    for edge in subpath:
        sub_tables.add(edge[0])
        sub_tables.add(edge[1])

    table_map = {t: cte_name for t in sub_tables}

    # Gather all query attributes for tables in this CTE
    final: dict = defaultdict(list)
    agg_items = [] # (table, column, agg) for aggregated metrics / filters

    for table in sub_tables:
        for key, items in tablematch[table].items():
            for item in items:
                if isinstance(item, list): # filter row
                    agg_items.append((item[2], item[1], item[5]))
                elif isinstance(item, dict): # metric / group by row
                    agg_items.append((item['table'], item['column'], item.get('agg')))
            final[key].extend(items)

    # SELECT aggregated expressions
    agg_select_parts = [
        f"{agg}({tbl}.{col}) AS opped_{col}"
        for tbl, col, agg in agg_items if agg
    ]

    # GROUP BY - user-requested groups and FK columns needed
    extra_group_items = []
    if cte_index < total_ctes - 1:
        # expose the FK that links to the next CTE
        extra_group_items.append({"table": subpath[-1][1], "column": subpath[-1][2]})
    if cte_index > 0:
        # expose the FK that links from the previous CTE
        extra_group_items.append({"table": subpath[0][1], "column": subpath[0][2]})

    group_string, group_col_set = _build_group_by_sql(final['group_by'] + extra_group_items)

    # Non-HAVING group cols go into SELECT too
    plain_group_cols = [c for c in group_col_set if not c.startswith("HAVING")]

    select_parts = agg_select_parts + plain_group_cols
    if cte_index < total_ctes - 1:
        select_parts.append(f"{subpath[-1][1]}.{subpath[-1][2]}")
    if cte_index > 0:
        select_parts.append(f"{subpath[0][1]}.{subpath[0][2]}")

    select_string = "\nSELECT\n\t" + ",\n\t".join(select_parts)

    join_string   = get_path_as_sql(subpath)
    criteria, having = _build_where_and_having(final['filter'], real, params)

    where_string = f"\nWHERE {_criterion_to_sql(criteria)}" if criteria else ""
    having_string = f"\nHAVING {_criterion_to_sql(having)}" if having  else ""

    def indent(s):
        return s.replace('\n', '\n\t')

    cte_body = (
        f"\n{cte_name} AS ("
        + indent(select_string)
        + indent(join_string)
        + indent(where_string)
        + indent(group_string)
        + indent(having_string)
        + "\n)"
    )
    return cte_body, table_map


# Public entry point

def build_sql_from_request(request, real: bool = True) -> tuple[str, list] | str:
    """
    Convert a QueryBuilder instance into a SQL string (and parameter list when real=True).

    Pipeline:
      1. Resolve field names via semantic matching
      2. Find shortest join path, splitting into CTEs where needed
      3. Build CTE blocks if necessary
      4. Build the final SELECT, WHER, GROUP BY, ORDER BY
      5. Assemble and return
    """
    filters = request.filters
    metrics = request.metrics
    rank = request.rank
    limit = request.limit
    group_by = request.group_by

    params: list = []
    aggs: set = set()

    # Resolve field names

    resolved_metrics = [{**m, **_resolve_column(m["field"])} for m in metrics]
    for m in resolved_metrics:
        tablematch_entry = m  # kept for clarity below
        if m.get("agg"):
            aggs.add(m["table"])

    resolved_rank = None
    if rank:
        resolved_rank = {**rank, **_resolve_column(rank["column"])}
        if resolved_rank.get("agg"):
            aggs.add(resolved_rank["table"])

    resolved_group_by = []
    if group_by:
        resolved_group_by = [{**g, **_resolve_column(g["field"])} for g in group_by]

    resolved_filters = [{**f, **_resolve_column(f["column"])} for f in filters]

    # Build tablematch and flat filter list

    tablematch: dict = defaultdict(lambda: defaultdict(list))

    for m in resolved_metrics:
        tablematch[m["table"]]["metric"].append(m)

    if resolved_rank:
        tablematch[resolved_rank["table"]]["rank"].append(resolved_rank)

    for g in resolved_group_by:
        tablematch[g["table"]]["group_by"].append(g)

    filter_rows = [] # flat [concat, col, table, op, value, agg] for final WHERE
    for f in resolved_filters:
        row = [f["concat"], f["column"], f["table"], f["op"], f["value"], f.get("agg")]
        if f.get("agg"):
            aggs.add(f["table"])
        tablematch[f["table"]]["filter"].append(row)
        filter_rows.append(row)

    # Find join path

    tables = set(tablematch.keys())
    path_segments = get_shortest_path(tables, aggs)

    # Build CTEs if the path was split

    cte_string = ""
    table_map: dict = {} # original table name to CTE name
    split = len(path_segments) > 1

    if split:
        cte_blocks = []
        cte_names  = [
            seg[0][0][:3].capitalize() + seg[-1][1][:3].capitalize() # first three characters of first and last tables
            for seg in path_segments
        ]
        new_path = []

        for idx, (subpath, cte_name) in enumerate(zip(path_segments, cte_names)):
            cte_block, sub_map = _build_cte(
                subpath, cte_name, idx, len(path_segments),
                tablematch, real, params
            )
            cte_blocks.append(cte_block)
            table_map.update(sub_map)

            if idx < len(path_segments) - 1:
                new_path.append([cte_name, cte_names[idx + 1], subpath[-1][2]])

            # point rank to the CTE name if its source table is in this segment
            if resolved_rank and resolved_rank["table"] in sub_map:
                resolved_rank = {**resolved_rank, "table": cte_name}

        cte_string = "WITH" + ",\n".join(cte_blocks) + "\n\n"
        final_path = new_path
    else:
        final_path = path_segments[0] if path_segments else []

    # Build outer WHERE or HAVING
    # Only keep filters whose table wasn't absorbed into a CTE
    outer_filter_rows = [r for r in filter_rows if table_map.get(r[2]) is None]
    outer_criteria, outer_having = _build_where_and_having(outer_filter_rows, real, params)

    where_string  = f"\nWHERE\n\t{_criterion_to_sql(outer_criteria)}" if outer_criteria else ""
    having_string = f"\nHAVING {_criterion_to_sql(outer_having)}" if outer_having  else ""

    # ORDER BY
    
    order_string = ""
    if resolved_rank:
        r = resolved_rank
        r_table = table_map.get(r["table"], r["table"])
        if r.get("agg") is None:
            order_string = f"\nORDER BY {r_table}.{r['column']} {r['order']}"
        elif split:
            order_string = f"\nORDER BY {r_table}.opped_{r['column']} {r['order']}"
        else:
            order_string = f"\nORDER BY {r['agg']}({r_table}.{r['column']}) {r['order']}"

    if limit:
        order_string += f"\nLIMIT {limit}"

    # SELECT
    # Remap metric tables if they ended up in a CTE
    final_metrics = [{**m, "table": table_map.get(m["table"], m["table"])} for m in resolved_metrics]
    final_group_by = [{**g, "table": table_map.get(g["table"], g["table"])} for g in resolved_group_by]

    group_cols_select = [f"{g['table']}.{g['column']}" for g in final_group_by]

    metric_select_parts = []
    for m in final_metrics:
        if m.get("agg"):
            if split:
                metric_select_parts.append(f"{m['table']}.opped_{m['column']}")
            else:
                metric_select_parts.append(f"{m['agg']}({m['table']}.{m['column']})")
        else:
            metric_select_parts.append(f"{m['table']}.{m['column']}")

    if group_cols_select or metric_select_parts:
        all_select = group_cols_select + metric_select_parts
        select_string = f"SELECT DISTINCT\n\t{',\n\t'.join(all_select)}"
    else:
        select_string = "SELECT DISTINCT *"

    # GROUP BY 

    outer_group_by_items = [g for g in final_group_by if table_map.get(g.get("table")) is None]
    group_string, _ = _build_group_by_sql(outer_group_by_items)

    # Assemble

    join_string = get_path_as_sql(final_path)

    sql = (
        cte_string
        + select_string
        + join_string
        + where_string
        + group_string
        + having_string
        + order_string
        + ";"
    )

    return (sql, params) if real else sql