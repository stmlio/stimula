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
"""

import argparse

from stimula.cli import local, remote


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
    # check if mapping is provided as file name, then use the first line
    if args.mapping and args.mapping.endswith('.csv'):
        with open(args.mapping, 'r') as file:
            args.mapping = file.readline().strip()


def main():
    parser = argparse.ArgumentParser(description='Stimula CLI')
    parser.add_argument('command', help='Command to execute', choices=['auth', 'list', 'mapping', 'count', 'get', 'post'])
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
    parser.add_argument('-F', '--format', help='Response format', choices=['diff', 'sql'])
    args = parser.parse_args()

    if args.remote:
        # if remote is specified, use remote invoker
        invoker = remote.Invoker(args.remote)
    else:
        # otherwise, use local invoker
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
        table = invoker.get_table(args.table, args.mapping, args.query)
        print(table)
    elif args.command == 'post':
        assert args.file is not None, 'File is required for post command'
        enable = args.enable or ''

        # read file contents
        with args.file as file:
            contents = file.read()

        if args.mapping is None and args.skip > 0:
            # use first line of contents as mapping
            args.mapping = contents.splitlines()[0]

        csv = invoker.post_table(args.table, args.mapping, args.query, contents,
                                 skiprows=args.skip,
                                 insert='I' in enable,
                                 update='U' in enable,
                                 delete='D' in enable,
                                 execute='E' in enable,
                                 commit='C' in enable,
                                 format=args.format or 'diff')
        print(csv)


def validate_flags(value):
    valid_letters = set("IUDEC")
    input_set = set(value)

    if not input_set.issubset(valid_letters):
        raise argparse.ArgumentTypeError(f"Invalid combination: {value}. Only the letters I, U, D, E, and C are allowed.")

    return value


if __name__ == '__main__':
    main()
