from stimula.service.query_executor import OperationType


def test_post_multiple_tables_get_full_report(db, books, context):
    # verify that we can post books and authors in a single request, even if there are complex dependencies
    table_names = ['authors', 'books']
    contexts = ['authors.csv', 'books.csv']
    # Rename Dickens, this causes insert and delete
    authors = '''name[unique=true]
        Jane Austen
        Leo Tolstoy
        Joseph Heller
        Charles John Huffam Dickens
    '''
    # relink David Copperfield to Charles Dickens
    books = '''title[unique=true], authorid(name)
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch-22, Joseph Heller
        David Copperfield, Charles John Huffam Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Leo Tolstoy
    '''
    # make list of utf-8 encoded binary strings
    body = [authors.encode('utf-8'), books.encode('utf-8')]
    full_report = db.post_multiple_tables_get_full_report(table_names, None, None, body, skiprows=1, insert=True, update=True, delete=True, execute=True, context=contexts)
    expected = {
        'summary': {
            'execute': True, 'commit': False,
            'found': {'insert': 1, 'update': 1, 'delete': 1},
            'success': {'insert': 1, 'update': 1, 'delete': 1},
            'failed': {'insert': 0, 'update': 0, 'delete': 0}
        },
        'rows': [
            {'line_number': 3, 'operation_type': OperationType.INSERT, 'success': True, 'rowcount': 1, 'table': 'authors', 'context': 'authors.csv',
             'query': 'insert into authors(name) select :name',
             'params': {'name': 'Charles John Huffam Dickens'}},
            {'line_number': 3, 'operation_type': OperationType.UPDATE, 'success': True, 'rowcount': 1, 'table': 'books', 'context': 'books.csv',
             'query': 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name',
             'params': {'title': 'David Copperfield', 'name': 'Charles John Huffam Dickens'}},
            {'operation_type': OperationType.DELETE, 'success': True, 'rowcount': 1, 'table': 'authors', 'context': 'authors.csv',
             'query': 'delete from authors where authors.name = :name',
             'params': {'name': 'Charles Dickens'}}
        ]}

    assert full_report == expected
