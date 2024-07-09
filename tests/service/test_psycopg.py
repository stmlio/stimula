from sqlalchemy import MetaData, create_engine


def test_cursor(cnx):
    # create cursor
    cursor = cnx.cursor()
    # verify cursor is not None
    assert cursor is not None
    # close cursor
    cursor.close()
    # assert cursor is closed
    assert cursor.closed == 1


def test_execute_select(cnx):
    # create cursor
    cursor = cnx.cursor()
    # execute select statement
    cursor.execute("SELECT 1")
    # verify result
    assert cursor.fetchone()[0] == 1
    # close cursor
    cursor.close()


def test_insert(cnx, test_table):
    cursor = cnx.cursor()
    # count rows
    cursor.execute("SELECT COUNT(*) FROM test")
    # verify result
    assert cursor.fetchone()[0] == 0
    # execute insert statement
    cursor.execute("INSERT INTO test VALUES (1)")
    # count rows
    cursor.execute("SELECT COUNT(*) FROM test")
    # verify result
    assert cursor.fetchone()[0] == 1


def test_cursor_close(cnx, test_table):
    with cnx.cursor() as cursor:
        # execute insert statement
        cursor.execute("INSERT INTO test VALUES (1)")
    # verify cursor is closed
    assert cursor.closed == 1


def test_commit_on_cursor_close(cnx, test_table):
    with cnx.cursor() as cursor:
        # execute insert statement
        cursor.execute("INSERT INTO test VALUES (1)")
    with cnx.cursor() as cursor:
        # count rows
        cursor.execute("SELECT COUNT(*) FROM test")
        # verify result
        assert cursor.fetchone()[0] == 1


def test_rollback(cnx, test_table):
    with cnx.cursor() as cursor:
        # count rows
        cursor.execute("SELECT COUNT(*) FROM test")
        # verify result
        assert cursor.fetchone()[0] == 0
    with cnx.cursor() as cursor:
        # execute insert statement
        cursor.execute("INSERT INTO test VALUES (1)")
        # rollback
        cnx.rollback()
    with cnx.cursor() as cursor:
        # count rows
        cursor.execute("SELECT COUNT(*) FROM test")
        # verify result
        assert cursor.fetchone()[0] == 0


def test_list_tables(cnx, test_table):
    with cnx.cursor() as cursor:
        # list tables
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'test'")
        # verify result
        assert cursor.fetchone()[0] == 'test'


def test_list_columns(cnx, test_table):
    with cnx.cursor() as cursor:
        # list tables
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'test'")
        # verify result
        assert cursor.fetchone()[0] == 'id'

