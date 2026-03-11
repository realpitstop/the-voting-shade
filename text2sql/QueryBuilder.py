class QueryBuilder:
    """
    Class to be used to assemble queries that can be converted into full SQL commands
    """
    def __init__(self, filters=None, metrics=None, group_by=None, rank=None, limit=250):
        if group_by is None:
            group_by = []
        if metrics is None:
            metrics = []
        if filters is None:
            filters = []

        self.filters = filters
        self.metrics = metrics
        self.group_by = group_by
        self.rank = rank
        self.limit = limit

    def add_filter(self, column, value, op="=", concat="AND", agg=None):
        """
        Add a filter
        in SQL: WHERE column (==, <, >, etc.) value
        """
        if not self.filters:
            concat = None

        self.filters.append({
            "concat": concat,
            "column": column,
            "op": op,
            "value": value,
            "agg": agg
        })
        return self

    def add_metric(self, field, agg=None):
        """
        Add a metric- the thing you're trying to look at
        in SQL: SELECT column1, agg(column2)
        """
        self.metrics.append({"field": field, "agg": agg})
        return self

    def add_group(self, field, value=None, op=None,  agg=None):
        """
        Add a grouping mechanism: how you want to split up the data
        in SQL: GROUP BY column1, agg(column2)
        """
        self.group_by.append({
            "field": field,
            "value": value,
            "op": op,
            "agg": agg,
        })
        return self

    def set_rank(self, column, order="DESC", agg=None):
        """
        Add a ranking mechanism: how to order the data
        in SQL: ORDER BY column (ASC/DESC)
        """
        self.rank = {"column": column, "order": order, "agg": agg}
        return self