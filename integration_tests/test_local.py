import glob
import sys
from unittest.mock import patch

from stimula.cli.cli import main

DB_NAME = 'afas18'
DB_USER = 'odoo'
DB_PASS = 'odoo'
STIMULA_KEY = "secret"


def test_auth():
    call_main(f'stimula auth -k {STIMULA_KEY} -d {DB_NAME} -u {DB_USER} -p {DB_PASS} -V')


# make stimula believe it's in TTY mode, even though it's started from test runner
@patch('sys.stdin.isatty', return_value=True)
def test_post_single_file(mock_isatty):
    call_main(f'stimula post -k {STIMULA_KEY} -t res_partner -f csv/res_partner.csv -IUC -V')


@patch('sys.stdin.isatty', return_value=True)
def test_post_attachment(mock_isatty):
    call_main(f'stimula post -k {STIMULA_KEY} -t ir_attachment -f csv/ir_attachment.csv -IUC -V')


def test_pipe_single_file():
    # Open a file to simulate piping input
    with open('csv/res_users.csv', 'r') as f:
        with patch('sys.stdin', f):
            call_main(f'stimula post -k {STIMULA_KEY} -t res_users -IU -V')


@patch('sys.stdin.isatty', return_value=True)
def test_post_multiple_files(mock_isatty):
    call_main(f'stimula post -k {STIMULA_KEY} -t res_users res_partner -f csv/res_users.csv csv/res_partner.csv -IU')


@patch('sys.stdin.isatty', return_value=True)
def test_post_stml_file(mock_isatty):
    call_main(f'stimula post -k {STIMULA_KEY} -f csv/res_partner.stml -IU -V')


@patch('sys.stdin.isatty', return_value=True)
def _test_post_stml_file(mock_isatty):
    call_main(f'stimula post -k {STIMULA_KEY} -f beauty/customer.stml -IU -V')


@patch('sys.stdin.isatty', return_value=True)
def test_post_with_wildcard(mock_isatty):
    paths = ' '.join(glob.glob('demo/face/*.csv'))
    call_main(f'stimula post -k {STIMULA_KEY} -f {paths} -IU -V')


def test_anonymize():
    # Open a file to simulate piping input
    filename = '../pgs/customer.csv'

    # skip test if filename does not exist
    if not glob.glob(filename):
        print(f"Skipping test_anonymize because {filename} does not exist")
    with open(filename, 'r') as f:
        with patch('sys.stdin', f):
            call_main(f'stimula anonymize -V')


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
