from stimula.service.db import DB


def test_parameter(books, context):
    # verify that we can use a single parameter value to expand a placeholder
    table_name = 'books'
    header = 'title[unique=true], price[default-value=${price}]'
    where_clause = None
    body = 'Emma,'
    substitutions = 'domain, value, subst\nprice,12.99'

    result = DB()._get_diffs_and_sql(table_name, header, where_clause, body, 0, None, False, True, False, None, table_name, substitutions=substitutions)

    # check there's one query
    assert len(result[1]) == 1
    executor = result[1][0]
    # check the params
    assert executor.params == {'price': 12.99, 'title': 'Emma'}


def test_parameter_multiple_values(books, context):
    # verify that we can use multiple parameter values to expand a placeholder
    table_name = 'books'
    header = 'title[unique=true: default-value=${title_param}]'
    where_clause = None
    body = ','
    substitutions = 'domain, value, subst\ntitle_param, book 1\ntitle_param, book 2'

    result = DB()._get_diffs_and_sql(table_name, header, where_clause, body, 0, None, True, False, False, None, table_name, substitutions=substitutions)

    # check there are two queries
    executors = result[1]
    assert len(executors) == 2
    # check the params
    assert executors[0].params == {'title': 'book 1'}
    assert executors[1].params == {'title': 'book 2'}

def test_multiple_parameters_multiple_values(books, context):
    # verify that we can use multiple parameter values to expand the same placeholder multiple times
    table_name = 'books'
    header = 'title[unique=true: default-value=${title_param}], seriesid(title)[default-value=${title_param}]'
    where_clause = None
    body = ','
    substitutions = 'domain, value, subst\ntitle_param, book 1\ntitle_param, book 2'

    result = DB()._get_diffs_and_sql(table_name, header, where_clause, body, 0, None, True, False, False, None, table_name, substitutions=substitutions)

    # check there are two queries
    executors = result[1]
    assert len(executors) == 2
    # check the params
    assert executors[0].params == {'title': 'book 1', 'title_1': 'book 1'}
    assert executors[1].params == {'title': 'book 2', 'title_1': 'book 2'}

def test_parameter_in_key_and_expression(books, context):
    # verify that we can use parameter values to expand placeholder in keys and expressions
    table_name = 'properties'
    header = 'title[unique=true], price[default-value=${price}]'
    header = 'name[unique=true], jsonb[key=${company_id}: exp="${company_id}"]'
    where_clause = None
    body = 'Emma,'
    substitutions = 'domain, value, subst\ncompany_id,1\ncompany_id,2'

    result = DB()._get_diffs_and_sql(table_name, header, where_clause, body, 0, None, True, True, False, None, table_name, substitutions=substitutions)

    # check there are two queries
    executors = result[1]
    assert len(executors) == 2
    # check the params
    assert executors[0].params == {'name': 'Emma', 'jsonb': '1'}
    assert executors[1].params == {'name': 'Emma', 'jsonb': '2'}
