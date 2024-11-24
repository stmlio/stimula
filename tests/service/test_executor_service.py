import time

from stimula.service.executor_service import ExecutorService
from stimula.service.query_executor import SimpleQueryExecutor, OperationType


def test_execute_sql_no_commit(db, books, context):
    sql = [
        SimpleQueryExecutor(0, OperationType.INSERT, 'books', 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
                            {'title': 'Catch XIII', 'name': 'Joseph Heller'}, 'books.csv'),
        SimpleQueryExecutor(1, OperationType.INSERT, 'books', 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
                            {'title': 'Witches', 'name': 'Charles Dickens'}, 'books.csv'),
        SimpleQueryExecutor(2, OperationType.UPDATE, 'books', 'update books set authorid = authors.author_id from authors where title = :title and authors.name = :name',
                            {'title': 'Anna Karenina', 'name': 'Leo Tolstoy'}, 'books.csv'),
        SimpleQueryExecutor(3, OperationType.DELETE, 'books', 'delete from books where title = :title', {'title': 'Catch-22'}, 'books.csv')
    ]
    result = ExecutorService().execute_sql(sql, True, False)

    rowcounts = [er.rowcount for er in result]
    expected = [1, 1, 1, 1]
    assert rowcounts == expected


def test_execute_sql_with_commit(db, books, context):
    sql = [
        SimpleQueryExecutor(0, OperationType.INSERT, 'books', 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
                            {'title': 'Catch XIII', 'name': 'Joseph Heller'}, 'books.csv'),
        SimpleQueryExecutor(1, OperationType.INSERT, 'books', 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
                            {'title': 'Witches', 'name': 'Charles Dickens'}, 'books.csv'),
        SimpleQueryExecutor(2, OperationType.UPDATE, 'books', 'update books set authorid = authors.author_id from authors where title = :title and authors.name = :name',
                            {'title': 'Anna Karenina', 'name': 'Leo Tolstoy'}, 'books.csv'),
        SimpleQueryExecutor(3, OperationType.DELETE, 'books', 'delete from books where title = :title', {'title': 'Catch-22'}, 'books.csv')
    ]
    result = ExecutorService().execute_sql(sql, True, True)
    rowcounts = [er.rowcount for er in result]
    expected = [1, 1, 1, 1]
    assert rowcounts == expected

def test_large_transaction(db, books, context):
    # create n insert queries
    create_query = lambda i : SimpleQueryExecutor(
        i,
        OperationType.INSERT,
        'books',
        'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
        {'title': f'Title {i}', 'name': 'Joseph Heller'},
        'books.csv')
    n = 2730

    sql = [create_query(i) for i in range(n)]
    # take start time
    start = time.time()

    # execute in one transaction
    result = ExecutorService().execute_sql(sql, True, True)

    # take end time
    end = time.time()
    # print total duration in s and average per query in ms
    print(f"Total duration: {end - start:.2f}s")
    print(f"Average duration per query: {(end - start) / n * 1000:.2f}ms")

    # check that all rows were inserted
    rowcounts = [er.rowcount for er in result]
    expected = [1] * n
    assert rowcounts == expected


