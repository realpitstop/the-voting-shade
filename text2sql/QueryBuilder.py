class QueryBuilder:
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
        self.metrics.append({"field": field, "agg": agg})
        return self

    def add_group(self, field, value=None, op=None,  agg=None):
        self.group_by.append({
            "field": field,
            "value": value,
            "op": op,
            "agg": agg,
        })
        return self

    def set_rank(self, column, order="DESC", agg=None):
        self.rank = {"column": column, "order": order, "agg": agg}
        return self