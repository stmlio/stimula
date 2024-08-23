from unittest.mock import patch

from tests.cli.test_local import call_main

REMOTE = 'http://localhost:8069/'
DB_NAME = 'beauty'
DB_USER = 'admin'
DB_PASS = 'admin'


def test_auth():
    call_main(f'stimula auth -r {REMOTE} -d {DB_NAME} -u {DB_USER} -p {DB_PASS}')


# make stimula believe it's in TTY mode, even though it's started from test runner
@patch('sys.stdin.isatty', return_value=True)
def test_post_single_file(mock_isatty):
    call_main(f'stimula post -r {REMOTE} -t res_partner -f cli/csv/res_partner.csv -e IUE')


def test_pipe_single_file():
    # Open a file to simulate piping input
    with open('cli/csv/res_users.csv', 'r') as f:
        with patch('sys.stdin', f):
            call_main(f'stimula post -r {REMOTE} -t res_users -e IUE')


@patch('sys.stdin.isatty', return_value=True)
def _test_post_multiple_files(mock_isatty):
    call_main(f'stimula post -r {REMOTE} -t res_users res_partner -f cli/csv/res_users.csv cli/csv/res_partner.csv -e IUE')
