def query_select(_name_table: str, _limit: int =10) -> str:
	# -- query_select.sql
    query = f"""
	with
		cte_query_select as (
            select *
            from s1.{_name_table}
            limit {_limit}
        )
            select * from cte_query_select
    """
    return query
