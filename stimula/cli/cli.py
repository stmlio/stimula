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
import json
import os
import sys
from importlib.metadata import version
from io import StringIO

import pandas as pd

from stimula.cli import local, remote
from stimula.cli.anonymizer import Anonymizer
from stimula.cli.file_source import FileSource
from stimula.cli.google_source import GoogleSource, google_authenticate
from stimula.service.query_executor import OperationType


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
                # print first line, to avoid the remote stacktrace
                first_line = str(e).split('\n')[0]
                print(f'Error: {first_line}')

            return 1

    def parse_args(self):
        parser = argparse.ArgumentParser(description='stimula - The STML CLI')
        parser.add_argument('command', help='Command to execute', choices=['auth', 'list', 'mapping', 'count', 'get', 'post', 'transpose', 'google', 'anonymize'])
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
        parser.add_argument('-F', '--format', help='Response format', choices=['diff', 'sql', 'full'], default='full')
        parser.add_argument('-v', '--version', action='version', version=version('stimula'))
        parser.add_argument('-V', '--verbose', action='store_true', help='Increase output verbosity')
        parser.add_argument('-M', '--transpose', action='store_true', help='Transpose the mapping')
        parser.add_argument('-x', '--execute', help='Script to execute on post')
        parser.add_argument('-c', '--context', nargs='+', help='Free text to match the query results, usually a source file name')
        parser.add_argument('-G', '--google_auth', nargs='?', help='Optional path of Google credentials file', const='client_secret.json')
        parser.add_argument('-g', '--google_sheet', nargs='?', help='ID of Google Sheets document')
        parser.add_argument('-a', '--audit', action='store_true', help='Print audit trail')
        parser.add_argument('-I', '--insert', action='store_true', help='Enable INSERT operations')
        parser.add_argument('-U', '--update', action='store_true', help='Enable UPDATE operations')
        parser.add_argument('-D', '--delete', action='store_true', help='Enable DELETE operations')
        parser.add_argument('-C', '--commit', action='store_true', help='Commit transaction')
        args = parser.parse_args()
        return args

    def execute_command(self, args):

        if args.command == 'transpose':
            # transpose stdin to stdout and exit
            self._transpose_stdin_stdout()
            return

        if args.command == 'anonymize':
            # transpose stdin to stdout and exit
            self._anonymize_stdin_stdout()
            return

        if args.command == 'google':
            assert args.google_auth, 'No Google credentials file provided. Use --google-auth or -G to provide Google credentials json file.'
            google_authenticate(args.google_auth)

        # if this is not auth, and no token provided, then try to read from local file
        if args.command != 'auth' and not args.token:
            self._read_token_from_file(args)

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
            if args.remote:
                print(f'Connected to {args.remote} as {args.user}.')
            else:
                print(f'Connected as {args.user}.')
        elif args.command == 'list':
            filter = args.tables[0] if args.tables else None
            tables = invoker.list(filter)
            # print name and count of tables
            for table in tables:
                if args.verbose or table["count"] > 0:
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
            assert args.insert or args.update or args.delete, 'At least one of the flags I, U, D must be enabled. Otherwise, there\'s nothing to do.'

            # read data from Google Sheets or from file
            if args.google_sheet:
                source = GoogleSource(args.google_sheet)
            else:
                source = FileSource()

            # read files from disk, stdin or google sheets. Also evaluate table and context
            files, tables, context, substitutions = source.read_files(args.files, args.tables, args.context)

            if args.mapping is None or args.mapping == '':
                assert args.skip > 0, 'No mapping provided and skip is zero. Specify a mapping using the --mapping or -m flag, or provide a file with a header row and --skip > 0.'
                assert not args.transpose, 'Cannot transpose mapping when reading header from data file.'
                # leave it to the server to use first line of contents as mapping

            # if verbose, print mapping
            if args.verbose and args.mapping:
                print(f'Mapping: {args.mapping}')

            result = invoker.post_table(tables, args.mapping, args.query, files,
                                        skiprows=args.skip,
                                        insert=args.insert,
                                        update=args.update,
                                        delete=args.delete,
                                        execute=True,
                                        commit=args.commit,
                                        format=args.format,
                                        post_script=args.execute,
                                        context=context,
                                        substitutions=substitutions)

            print(self._create_report(result, args.audit, args.verbose))

    def _authenticate(self, args, invoker):
        # assert that database and username are provided if we don't have a token
        if not args.token:
            # database is not required for single db Odoo instance
            assert args.user, 'Username must be provided for authentication.'

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
            if args.database:
                args.password = getpass.getpass(f'Enter password for {args.user}@{args.database}: ')
            else:
                args.password = getpass.getpass(f'Enter password for {args.user}: ')

        # authenticate and set token
        args.token = invoker.auth(args.database, args.user, args.password)

        # write token to local file
        self._write_token_to_file(args.token, args.remote)

    def _transpose_stdin_stdout(self):
        # validate we have stdin
        assert not sys.stdin.isatty(), 'No input provided, use piping to provide input.'
        # read dataframe from stdin
        df = pd.read_csv(sys.stdin, header=None)
        # transpose the dataframe and write to stdout
        df.T.to_csv(sys.stdout, header=False, index=False)

    def _anonymize_stdin_stdout(self):
        # validate we have stdin
        assert not sys.stdin.isatty(), 'No input provided, use piping to provide input.'
        # anonymize the dataframe
        Anonymizer().anonymize(sys.stdin, sys.stdout)

    def _read_token_from_file(self, args):
        # check if .stimula_token file exists
        if os.path.exists('.stimula_token'):
            try:
                # read json from .stimula_token file
                with open('.stimula_token', 'r') as file:
                    data = json.load(file)
                    args.token = data.get('token')
                    args.remote = data.get('remote')
            except json:
                print('Error reading token from file. Please re-authenticate.')
                pass

    def _write_token_to_file(self, token, remote):
        data = {'token': token}
        if remote:
            data['remote'] = remote
        # write token to local file
        with open('.stimula_token', 'w') as file:
            json.dump(data, file, indent=4)

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

    def _create_report(self, result, audit, verbose):
        if audit:
            return self._report_audit(result)
        elif verbose:
            return self._report_verbose(result)
        else:
            return self._report_summary(result)

    def _report_summary(self, result):
        summary = result.get('summary', {})
        total = summary.get('total', {})

        # report summary
        report = ''

        if 'rows' in summary:
            rows = summary.get("rows")
            report += f'Rows read:  {rows}\n'
            if rows == 0:
                return report

        operations = total.get("operations", 0)
        report += f'Operations: {operations}\n'
        if operations == 0:
            return report

        success = total.get('success', {})
        report += f'Success:    {success}\n'

        failed = total.get('failed', {})
        if failed > 0:
            report += f'Failed:     {failed}\n'

        error_rows = [row for row in result.get('rows', []) if not row.get('success', False)]
        error_report = '\n'.join([self._report_row(row, False) for row in error_rows])
        if error_report:
            report += f'{error_report}\n'

        if summary.get('commit', False):
            report += 'Transaction committed\n'
        else:
            report += 'Specify --commit (-C) to commit transaction.\n'

        return report

    def _report_verbose(self, result):

        summary = result.get('summary', {})
        total = summary.get('total', {})

        # report summary
        report = ''

        report += f'Rows read:  {summary.get("rows", 0)}\n'
        report += f'Operations: {total.get("operations", 0)}\n'
        success = summary.get('success', {})
        report += f'Success     {total.get("success", 0)} (insert: {success.get("insert", 0)}, update: {success.get("update", 0)}, delete: {success.get("delete", 0)})\n'
        failed = summary.get('failed', {})
        report += f'Failed      {total.get("failed", 0)} (insert: {failed.get("insert", 0)}, update: {failed.get("update", 0)}, delete: {failed.get("delete", 0)})\n'

        error_rows = [row for row in result.get('rows', []) if not row.get('success', False)]
        error_report = '\n'.join([self._report_row(row, True) for row in error_rows])

        if error_report:
            report += f'Errors:\n{error_report}\n'

        if summary.get('commit', False):
            report += 'Transaction committed\n'
        else:
            report += 'Specify --commit (-C) to commit transaction.\n'

        return report

    def _report_audit(self, result):
        # return result with indented json layout
        return json.dumps(result, default=self._custom_encoder, indent=2)

    def _custom_encoder(self, obj):
        if isinstance(obj, OperationType):
            # Convert OperationType Enum to string
            return repr(obj)
        elif isinstance(obj, pd.Timestamp):
            # Convert Timestamps to ISO format
            return obj.isoformat()
        # Fall back to default behavior for unknown types
        return json.JSONEncoder().default(obj)

    def _report_row(self, row, verbose):
        if not verbose:
            result = f'{row.get("context", "")}'
            # delete statements don't have a line number
            if row.get("line_number"):
                result += f':{row["line_number"]} - '
            result += f'"{row.get("error", "N/A")}" '
        else:
            result = f'{row.get("context", "")}'
            # delete statements don't have a line number
            if row.get("line_number"):
                result += f':{row["line_number"]} - '
            result += f'"{row.get("error", "N/A")}" - Query: "{row.get("query", "N/A")}" - Parameters: "{row.get("params", "N/A")}"'

        return result


def validate_flags(value):
    # validation function for enable flags
    valid_letters = set("iudec")
    # create a set from the input string, lowercase
    input_set = set(value.lower())

    if not input_set.issubset(valid_letters):
        raise argparse.ArgumentTypeError(f"Invalid combination: {value}. Only the letters I, U, D, E, and C are allowed.")

    return value


def main():
    return StimulaCLI().main()


if __name__ == '__main__':
    main()
