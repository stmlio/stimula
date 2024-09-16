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
-h, --help        Print help message
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
-v, --version     Print version information
-V, --verbose     Increase output verbosity
-M, --transpose   Transpose the mapping
-x, --execute     Script to execute on post
"""

import argparse
import getpass
import os
import sys
from importlib.metadata import version
from io import StringIO

import pandas as pd

from stimula.cli import local, remote
from stimula.cli.file_source import FileSource
from stimula.cli.google_source import GoogleSource, google_authenticate


class StimulaCLI:
    def main(self):
        # parse command line arguments
        args = self.parse_args()

        try:
            # execute command
            self.execute_command(args)

            return 0
        except Exception as e:
            if args.verbose:
                # print message with stack trace
                raise e
            else:
                # print message without stack trace to stderr
                print(f'Error: {e}', file=sys.stderr)
            return 1

    def parse_args(self):
        parser = argparse.ArgumentParser(description='stimula - The STML CLI')
        parser.add_argument('command', help='Command to execute', choices=['auth', 'list', 'mapping', 'count', 'get', 'post', 'transpose', 'google'])
        parser.add_argument('-r', '--remote', help='Remote API URL')
        parser.add_argument('-H', '--host', help='Database host', default='localhost')
        parser.add_argument('-P', '--port', help='Database port', type=int, default=5432)
        parser.add_argument('-d', '--database', help='Database name')
        parser.add_argument('-u', '--user', help='Database username')
        parser.add_argument('-p', '--password', help='Password')
        parser.add_argument('-k', '--key', help='Secret key')
        parser.add_argument('-T', '--token', help='Authentication token')
        parser.add_argument('-t', '--tables', nargs='+', help='One or more table names or a table name filter')
        parser.add_argument('-q', '--query', help='Query clause')
        parser.add_argument('-m', '--mapping', help='Mapping header')
        parser.add_argument('-f', '--files', nargs='+', help='One or more paths to the files to post')
        parser.add_argument('-s', '--skip', help='Number of rows to skip', type=int, default=1)
        parser.add_argument('-e', '--enable', help='Enable flags', type=validate_flags)
        parser.add_argument('-F', '--format', help='Response format', choices=['diff', 'sql', 'full'], default='full')
        parser.add_argument('-v', '--version', action='version', version=version('stimula'))
        parser.add_argument('-V', '--verbose', action='store_true', help='Increase output verbosity')
        parser.add_argument('-M', '--transpose', action='store_true', help='Transpose the mapping')
        parser.add_argument('-x', '--execute', help='Script to execute on post')
        parser.add_argument('-c', '--context', nargs='+', help='Free text to match the query results, usually a source file name')
        parser.add_argument('-G', '--google_auth', nargs='?', help='Optional path of Google credentials file', const='client_secret.json')
        parser.add_argument('-g', '--google_sheet', nargs='?', help='ID of Google Sheets document')
        args = parser.parse_args()
        return args

    def execute_command(self, args):

        if args.command == 'transpose':
            # transpose stdin to stdout and exit
            self._transpose_stdin_stdout()
            return

        if args.command == 'google':
            assert args.google_auth, 'No Google credentials file provided. Use --google-auth or -G to provide Google credentials json file.'
            google_authenticate(args.google_auth)

        if args.remote:
            # if remote is specified, use remote invoker
            invoker = remote.Invoker(args.remote)
        else:
            # otherwise, read key from environment if not provided as argument
            if not args.key:
                args.key = os.getenv('STIMULA_KEY')

            # if key is still not provided, raise an error
            assert args.key, 'Secret key must be provided for local connection. Either connect to remote API, or set --key argument, or STIMULA_KEY environment variable'

            # use local invoker
            invoker = local.Invoker(args.key, args.host, args.port)

        # try to read token from local file
        self._read_token_from_file(args)

        # if auth request or no token provided
        if args.command == 'auth' or not args.token:
            # authenticate and set token
            self._authenticate(args, invoker)

        # validate token and set connection context
        invoker.set_context(args.token)

        # check if mapping is provided as file name, then use the first line
        self._read_mapping_from_file(args)

        if args.transpose:
            # transpose mapping
            self._transpose_mapping(args)

        # use only first line of mapping
        if args.mapping:
            args.mapping = args.mapping.splitlines()[0]

        # execute command
        if args.command == 'auth':
            print(f'Token: {args.token}')
        elif args.command == 'list':
            filter = args.tables[0] if args.tables else None
            tables = invoker.list(filter)
            # print name and count of tables
            for table in tables:
                print(f'{table["name"]}: {table["count"]}')
        elif args.command == 'mapping':
            assert args.tables and len(args.tables) == 1, 'One table name must be provided using -t or --table flag.'
            mapping = invoker.mapping(args.tables[0])
            print(mapping)
        elif args.command == 'count':
            assert args.tables and len(args.tables) == 1, 'One table name must be provided using -t or --table flag.'
            count = invoker.count(args.tables[0], args.mapping, args.query)
            print(count)
        elif args.command == 'get':
            assert args.tables and len(args.tables) == 1, 'One table name must be provided using -t or --table flag.'
            table = invoker.get_table(args.tables[0], args.mapping, args.query)
            print(table)
        elif args.command == 'post':

            # we need at least one of the flags I, U, D,  enabled
            assert args.enable, 'At least one of the flags I, U, D must be enabled. Otherwise, there\'s nothing to do.'

            # read data from Google Sheets or from file
            if args.google_sheet:
                source = GoogleSource(args.google_sheet)
            else:
                source = FileSource()

            # read files from disk, stdin or google sheets. Also evaluate table and context
            files, tables, context = source.read_files(args.files, args.tables, args.context)

            if args.mapping is None or args.mapping == '':
                assert args.skip > 0, 'No mapping provided and skip is zero. Specify a mapping using the --mapping or -m flag, or provide a file with a header row and --skip > 0.'
                assert not args.transpose, 'Cannot transpose mapping when reading header from data file.'
                # leave it to the server to use first line of contents as mapping

            # if verbose, print mapping
            if args.verbose:
                print(f'Mapping: {args.mapping}')

            result = invoker.post_table(tables, args.mapping, args.query, files,
                                        skiprows=args.skip,
                                        insert='I' in args.enable,
                                        update='U' in args.enable,
                                        delete='D' in args.enable,
                                        execute='E' in args.enable,
                                        commit='C' in args.enable,
                                        format=args.format,
                                        post_script=args.execute,
                                        context=context)

            print(self._create_report(result, args.verbose))


    def _authenticate(self, args, invoker):
        # assert that database and username are provided if we don't have a token
        if not args.token:
            assert args.database and args.user, 'Database and username must be provided for authentication.'

        # if we have a token, then use it for default database and username
        if args.token:
            # get database and username from token
            database, user = invoker.get_database_and_username(args.token)
            # default to token values if not provided
            args.database = args.database or database
            args.user = args.user or user

        # ask for password if not provided
        if not args.password:
            # hide password input
            args.password = getpass.getpass(f'Enter password for {args.user}@{args.database}: ')
        # authenticate and set token
        args.token = invoker.auth(args.database, args.user, args.password)
        # write token to local file
        self._write_token_to_file(args.token)


    def _transpose_stdin_stdout(self):
        # validate we have stdin
        assert not sys.stdin.isatty(), 'No input provided, use piping to provide input.'
        # read dataframe from stdin
        df = pd.read_csv(sys.stdin, header=None)
        # transpose the dataframe and write to stdout
        df.T.to_csv(sys.stdout, header=False, index=False)

    def _read_token_from_file(self, args):
        # try to read token from local file if not provided in arguments
        if not args.token:
            try:
                with open('.stimula_token', 'r') as file:
                    args.token = file.read()
            except FileNotFoundError:
                pass

    def _write_token_to_file(self, token):
        # write token to local file
        with open('.stimula_token', 'w') as file:
            file.write(token)

    def _read_mapping_from_file(self, args):
        # check if args.mapping is an existing file name
        if args.mapping and os.path.exists(args.mapping):
            with open(args.mapping, 'r') as file:
                # read the whole file, it may be a transposed mapping
                args.mapping = file.read()

    def _transpose_mapping(self, args):
        assert args.mapping, 'Mapping must be provided when transposing.'
        # get string from args.mapping, remove lines starting with #
        mapping = '\n'.join(line for line in args.mapping.splitlines() if not line.startswith('#'))
        # use pandas to read the mapping string into a dataframe
        df = pd.read_csv(StringIO(mapping), header=None)
        # transpose the dataframe, dispose all but the first row and convert to csv
        args.mapping = df.T.head(1).to_csv(header=False, index=False)

    def _create_report(self, result, verbose):
        summary = result.get('summary', {})

        # report failed
        failed = summary.get('failed', {})
        total_failed = failed.get('insert', 0) + failed.get('update', 0) + failed.get('delete', 0)
        report_failed = 'Failed: '
        if total_failed == 0:
            report_failed += 'None\n'
        else:
            report_failed += f'{failed.get("insert", 0)} inserts, {failed.get("update", 0)} updates, {failed.get("delete", 0) } deletes\n'

        # report found
        found = summary.get('found', {})
        total_found = found.get('insert', 0) + found.get('update', 0) + found.get('delete', 0)
        report_found = 'Found: '
        if total_found == 0:
            report_found += 'None'
        else:
            report_found += f'{found.get("insert", 0)} inserts, {found.get("update", 0)} updates, {found.get("delete", 0)} deletes'

        # committed?
        committed = summary.get('commit', False)

        # report success
        success = summary.get('success', {})
        total_success = success.get('insert', 0) + success.get('update', 0) + success.get('delete', 0)
        report_success = 'Evaluated: ' if not committed else 'Committed: '
        if total_success == 0:
            report_success += 'None'
        else:
            report_success += f'{success.get("insert", 0)} inserts, {success.get("update", 0)} updates, {success.get("delete", 0)} deletes'


        all_rows = ''
        if not verbose:
            # report errors
            all_rows = '\n'.join([f'Line: {row["line_number"]} Error: {row["error"]}' for row in result.get('rows', []) if not row.get('success', False)])
        else:
            # report all rows
            all_rows = '\n'.join([f'File: {row["context"]} Line: {row["line_number"]} Success: {row.get("success", False)} Error: {row.get("error", "")} Query: {row["query"]}' for row in result.get('rows', [])])


        return (report_failed if total_failed > 0 else '') + (report_found if total_success == 0 else report_success) + ('\n' + all_rows if all_rows else '')


def validate_flags(value):
    # validation function for enable flags
    valid_letters = set("IUDEC")
    input_set = set(value)

    if not input_set.issubset(valid_letters):
        raise argparse.ArgumentTypeError(f"Invalid combination: {value}. Only the letters I, U, D, E, and C are allowed.")

    return value

def main():
    return StimulaCLI().main()

if __name__ == '__main__':
    main()
