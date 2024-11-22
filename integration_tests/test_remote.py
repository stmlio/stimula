import glob
import os
import sys
from unittest.mock import patch

from stimula.cli.cli import main

REMOTE = 'http://localhost:8069'
DB_NAME = 'afas18'
# REMOTE = 'https://stmlio-stimula-odoo-main-15312745.dev.odoo.com/'
# DB_NAME = 'stmlio-stimula-odoo-main-15312745'
DB_USER = 'admin'
DB_PASS = 'admin'


def test_auth():
    call_main(f'stimula auth -r {REMOTE} -d {DB_NAME} -u {DB_USER} -p {DB_PASS} -V')


# make stimula believe it's in TTY mode, even though it's started from test runner
@patch('sys.stdin.isatty', return_value=True)
def test_post_single_file(mock_isatty):
    call_main(f'stimula post -r {REMOTE} -t res_partner -f csv/res_partner.csv -IU -V')


@patch('sys.stdin.isatty', return_value=True)
def test_post_attachment(mock_isatty):
    call_main(f'stimula post -r {REMOTE} -t ir_attachment -f csv/ir_attachment.csv -IUC -V')


def test_pipe_single_file():
    # Open a file to simulate piping input
    with open('csv/res_users.csv', 'r') as f:
        with patch('sys.stdin', f):
            call_main(f'stimula post -r {REMOTE} -t res_users -IU')


@patch('sys.stdin.isatty', return_value=True)
def test_post_stml_file(mock_isatty):
    call_main(f'stimula post -r {REMOTE} -f csv/res_partner.stml -IU -V')


@patch('sys.stdin.isatty', return_value=True)
def test_post_multiple_files(mock_isatty):
    call_main(f'stimula post -r {REMOTE} -t res_users res_partner -f csv/res_users.csv csv/res_partner.csv -IUC -V')


@patch('sys.stdin.isatty', return_value=True)
def test_post_with_wildcard(mock_isatty):
    paths = ' '.join(glob.glob('csv/*.csv'))
    call_main(f'stimula post -r {REMOTE} -f {paths} -IU -V')


def call_main(cmd):
    # Backup the original sys.argv
    original_argv = sys.argv

    # Simulate command-line arguments
    sys.argv = cmd.split()

    # Call main() and capture the result
    result = main()

    # Restore the original sys.argv
    sys.argv = original_argv

    assert result == 0
