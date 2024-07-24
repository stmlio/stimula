"""
This script provides a command-line interface (CLI) for interacting with the Stimula API.
It supports both local and remote invocations, allowing users to execute a variety of commands such as
authentication, listing tables, retrieving mappings, counting records, fetching tables, and posting data.

Author: Romke Jonker
Email: romke@rnadesign.net

Usage:
python cli.py <command> [options]

Commands:
- auth: Authenticate, retrieve a token and store it in a local file.
- list: List tables and their record counts.
- mapping: Retrieve mapping for a specified filter.
- count: Count records in a table based on specified criteria.
- get: Fetch a table with specified mapping and query parameters.
- post: Post data to a table from a specified file.

Options:
-r, --remote      Remote API URL
-H, --host        Database host (default: localhost)
-P, --port        Database port (default: 5432)
-d, --database    Database name
-u, --user        Database username
-p, --password    Password
-k, --key         Secret key
-T, --token       Authentication token
-t, --table       Table name or filter
-q, --query       Query clause
-m, --mapping     Mapping header
-f, --file        Path to the file to post (required for post command)
-s, --skip        Number of rows to skip (default: 1)
-e, --enable      Enable flags (I, U, D, E, C)
-F, --format      Response format (choices: diff, sql)
-D, --deduplicate Deduplicate records based on primary key
-v, --version     Print version information
-V, --verbose     Increase output verbosity
-M, --transpose   Transpose the mapping
"""

import argparse
import os
import sys
from importlib.metadata import version
from io import StringIO

import pandas as pd

from stimula.cli import local, remote


def main():
    parser = argparse.ArgumentParser(description='Stimula CLI')
    parser.add_argument('command', help='Command to execute', choices=['auth', 'list', 'mapping', 'count', 'get', 'post', 'transpose'])
    parser.add_argument('-r', '--remote', help='Remote API URL')
    parser.add_argument('-H', '--host', help='Database host', default='localhost')
    parser.add_argument('-P', '--port', help='Database port', type=int, default=5432)
    parser.add_argument('-d', '--database', help='Database name')
    parser.add_argument('-u', '--user', help='Database username')
    parser.add_argument('-p', '--password', help='Password')
    parser.add_argument('-k', '--key', help='Secret key')
    parser.add_argument('-T', '--token', help='Authentication token')
    parser.add_argument('-t', '--table', help='Table name or filter')
    parser.add_argument('-q', '--query', help='Query clause')
    parser.add_argument('-m', '--mapping', help='Mapping header')
    parser.add_argument('-f', '--file', help='Path to the file to post', type=argparse.FileType('r'))
    parser.add_argument('-s', '--skip', help='Number of rows to skip', type=int, default=1)
    parser.add_argument('-e', '--enable', help='Enable flags', type=validate_flags)
    parser.add_argument('-F', '--format', help='Response format', choices=['diff', 'sql'], default='sql')
    parser.add_argument('-D', '--deduplicate', action='store_true', help='Deduplicate by unique key')
    parser.add_argument('-v', '--version', action='version', version=version('stimula'))
    parser.add_argument('-V', '--verbose', action='store_true', help='Increase output verbosity')
    parser.add_argument('-M', '--transpose', action='store_true', help='Transpose the mapping')

    args = parser.parse_args()

    try:
        execute_command(args)
    except Exception as e:
        if args.verbose:
            # print message with stack trace
            raise e
        else:
            # print message without stack trace to stderr
            print(f'Error: {e}', file=sys.stderr)

        sys.exit(1)


def execute_command(args):

    if args.command == 'transpose':
        # transpose stdin to stdout and exit
        _transpose_stdin_stdout()
        return

    if args.remote:
        # if remote is specified, use remote invoker
        invoker = remote.Invoker(args.remote)
    else:
        # otherwise, read key from environment if not provided as argument
        if not args.key:
            args.key = os.getenv('STIMULA_KEY')

        # if key is still not provided, raise an error
        assert args.key, 'Secret key must be provided, either as --key argument or STIMULA_KEY environment variable'

        # use local invoker
        invoker = local.Invoker(args.key, args.host, args.port)

    # try to read token from local file
    _read_token_from_file(args)

    # if auth request or no token provide
    if args.command == 'auth' or not args.token:
        # authenticate and set token
        args.token = invoker.auth(args.database, args.user, args.password)
        #     # write token to local file
        _write_token_to_file(args.token)

    # validate token and set connection context
    invoker.set_context(args.token)

    # check if mapping is provided as file name, then use the first line
    _read_mapping_from_file(args)

    if args.transpose:
        # transpose mapping
        _transpose_mapping(args)

    # use only first line of mapping
    if args.mapping:
        args.mapping = args.mapping.splitlines()[0]

    # execute command
    if args.command == 'auth':
        print(f'Token: {args.token}')
    elif args.command == 'list':
        tables = invoker.list(args.table)
        # print name and count of tables
        for table in tables:
            print(f'{table["name"]}: {table["count"]}')
    elif args.command == 'mapping':
        mapping = invoker.mapping(args.table)
        print(mapping)
    elif args.command == 'count':
        count = invoker.count(args.table, args.mapping, args.query)
        print(count)
    elif args.command == 'get':
        assert args.table, 'Table name must be provided using -t or --table flag.'
        table = invoker.get_table(args.table, args.mapping, args.query)
        print(table)
    elif args.command == 'post':
        assert args.table, 'Table name must be provided using -t or --table flag.'
        assert args.enable, 'At least one of the flags I, U, D, E, or C must be enabled. Otherwise, there\'s nothing to do.'

        # read file contents
        if args.file:
            with args.file as file:
                contents = file.read()
        elif not sys.stdin.isatty():
            # Input is being piped in
            contents = sys.stdin.read()
        else:
            contents = None

        # raise error if no contents are provided
        assert contents, 'No contents provided, either use --file or -f flag, or pipe data to stdin.'

        if args.mapping is None or args.mapping == '':
            assert args.skip > 0, 'No mapping provided and skip is zero. Specify a mapping using the --mapping or -m flag, or provide a file with a header row and --skip > 0.'
            assert not args.transpose, 'Cannot transpose mapping when reading header from data file.'
            # use first line of contents as mapping
            args.mapping = contents.splitlines()[0]

        # if verbose, print mapping
        if args.verbose:
            print(f'Mapping: {args.mapping}')

        csv = invoker.post_table(args.table, args.mapping, args.query, contents,
                                 skiprows=args.skip,
                                 insert='I' in args.enable,
                                 update='U' in args.enable,
                                 delete='D' in args.enable,
                                 execute='E' in args.enable,
                                 commit='C' in args.enable,
                                 format=args.format or 'diff',
                                 deduplicate=args.deduplicate)
        print(csv)


def validate_flags(value):
    valid_letters = set("IUDEC")
    input_set = set(value)

    if not input_set.issubset(valid_letters):
        raise argparse.ArgumentTypeError(f"Invalid combination: {value}. Only the letters I, U, D, E, and C are allowed.")

    return value


def _transpose_stdin_stdout():
    # validate we have stdin
    assert not sys.stdin.isatty(), 'No input provided, use piping to provide input.'
    # read dataframe from stdin
    df = pd.read_csv(sys.stdin, header=None)
    # transpose the dataframe and write to stdout
    df.T.to_csv(sys.stdout, header=False, index=False)


def _read_token_from_file(args):
    # try to read token from local file if not provided in arguments
    if not args.token:
        try:
            with open('.stimula_token', 'r') as file:
                args.token = file.read()
        except FileNotFoundError:
            pass


def _write_token_to_file(token):
    # write token to local file
    with open('.stimula_token', 'w') as file:
        file.write(token)


def _read_mapping_from_file(args):
    # check if args.mapping is an existing file name
    if args.mapping and os.path.exists(args.mapping):
        with open(args.mapping, 'r') as file:
            # read the whole file, it may be a transposed mapping
            args.mapping = file.read()


def _transpose_mapping(args):
    assert args.mapping, 'Mapping must be provided when transposing.'
    # get string from args.mapping, remove lines starting with #
    mapping = '\n'.join(line for line in args.mapping.splitlines() if not line.startswith('#'))
    # use pandas to read the mapping string into a dataframe
    df = pd.read_csv(StringIO(mapping), header=None)
    # transpose the dataframe, dispose all but the first row and convert to csv
    args.mapping = df.T.head(1).to_csv(header=False, index=False)


if __name__ == '__main__':
    main()
