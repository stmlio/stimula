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

