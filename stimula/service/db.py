"""
This class provides the main database service to list tables, create mappings, retrieve table contents and to post updates.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import logging
import re
from io import StringIO

import pandas as pd
import psycopg2
from pandas import DataFrame

from stimula.compiler.header_compiler import HeaderCompiler
from stimula.compiler.select_compiler import SelectCompiler
from stimula.compiler.types_compiler import TypesCompiler
from stimula.header.csv_header_parser import HeaderParser
from stimula.header.header_lexer import HeaderLexer
from stimula.header.header_merger import HeaderMerger
from stimula.header.odoo_header_parser import OdooHeaderParser
from .context import cnx_context, get_metadata
from .diff_to_sql import DiffToSql

_logger = logging.getLogger(__name__)


class DB:
    def __init__(self):
        self._lexer = HeaderLexer()
        self._diff_to_sql = DiffToSql()

    def get_tables(self, filter=None):

        cr = cnx_context.cr

        # query to get all tables, excluding views
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"

        cr.execute(query)

        # fetch all rows
        all_tables = cr.fetchall()

        if filter is None:
            # return all tables
            unsorted = [t for t in all_tables]
        else:
            # use filter as a regular expression
            regex = re.compile(filter)
            unsorted = [t for t in all_tables if regex.search(t[0])]

        # get row counts
        with_counts = self.get_counts(unsorted)

        # return sorted(with_count)
        sorted_tables = sorted(with_counts, key=lambda e: e[0])

        # convert to dictionary for JSON serialization
        return [{'name': table_name, 'count': row_count} for table_name, row_count in sorted_tables]

    def get_counts(self, tables):
        cr = cnx_context.cr
        for table in tables:
            query = f"SELECT COUNT(*) FROM {table[0]}"
            cr.execute(query)
            row_count = cr.fetchone()[0]
            yield table[0], row_count

    def get_count(self, table_name, header, where_clause):
        # create metadata object from context
        metadata = get_metadata(cnx_context.cnx)

        if not header:
            # if header parameter is empty, then use the default header
            default_mapping = OdooHeaderParser(metadata, cnx_context.cr).parse(table_name)

            # remove columns that are not enabled
            mapping = {**default_mapping, 'columns': [c for c in default_mapping['columns'] if c.get('enabled', False)]}

        else:
            # parse header to build syntax tree
            mapping = HeaderParser(metadata, table_name).parse_csv(header)

        query = SelectCompiler().compile_count_query(mapping, where_clause)

        cr = cnx_context.cr

        # execute query
        cr.execute(query)

        # fetch result
        result = cr.fetchone()

        return result[0]

    def get_table(self, table_name, header, where_clause=None):
        # create metadata object from context
        metadata = get_metadata(cnx_context.cnx)

        if not header:
            # if header parameter is empty, then use the default header
            default_mapping = OdooHeaderParser(metadata, cnx_context.cr).parse(table_name)

            # remove columns that are not enabled
            mapping = {**default_mapping, 'columns': [c for c in default_mapping['columns'] if c.get('enabled', False)]}

        else:
            # parse header to build syntax tree
            mapping = HeaderParser(metadata, table_name).parse_csv(header)

        df = self._read_from_db(mapping, where_clause)

        return df, mapping

    def get_table_as_csv(self, table_name, header, where_clause, escapechar=''):
        # get table as dataframe
        df, mapping = self.get_table(table_name, header, where_clause)

        return self.convert_to_csv(df, mapping, escapechar)

    def convert_to_csv(self, df, mapping, escapechar=''):
        # need converters from types compiler
        column_names = HeaderCompiler().compile_list(mapping)
        column_types = TypesCompiler().compile(mapping, column_names)

        # get converters from column_types
        converters = column_types['write_csv_converters']

        # apply converters to dataframe
        for column in column_names:
            if column in converters:
                df[column] = df[column].apply(converters[column])

        # convert dataframe to csv
        return df.to_csv(index=False, escapechar=escapechar)

    def post_table_get_diff(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False, deduplicate=False):
        # create diffs and sql
        diffs, sql = self._get_diffs_and_sql(table_name, header, where_clause, body, skiprows, insert, update, delete, deduplicate)
        # if execute
        if execute:
            # execute sql statements
            results_tuple = self._execute_sql(sql, commit)
            # concatenate results
            results = [result for results in results_tuple for result in results]
            # zip row counts with diffs statements
            for diff, result in zip(diffs, results):
                diff.append(result.rowcount)

        return diffs

    def post_table_get_sql(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False, deduplicate=False):
        # create diffs and sql
        _, sql = self._get_diffs_and_sql(table_name, header, where_clause, body, skiprows, insert, update, delete, deduplicate)

        # sort queries
        sorted_queries = self._sort_queries(sql)

        if execute:
            # execute sql statements
            sqls = self._execute_sql(sorted_queries, commit)
        else:
            sqls = [(None, qe.query, qe.params) for qe in sorted_queries]

        # convert sql to dataframe
        return self._convert_to_df(sqls, execute)

    def _convert_to_df(self, sqls, showResult):
        # create empty pandas dataframe. First column contains sql
        if showResult:
            result = pd.DataFrame(columns=['rows', 'sql'])
        else:
            result = pd.DataFrame(columns=['sql'])

        # iterate sql statements
        index = 0
        for rows, query, value_dict in sqls:
            # create dictionary with query and value_dict
            value_dict = {**value_dict, 'sql': query}
            if showResult:
                value_dict = {**value_dict, 'rows': rows}
            # append a new row to the bottom result with values from value dictionary using concat
            result = pd.concat([result, pd.DataFrame(value_dict, index=[index])], ignore_index=True)
            index += 1

        # force pandas to not convert int columns to float if they contain NaNs
        result = result.convert_dtypes()

        # return dataframe
        return result

    def post_table_get_summary(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False):
        with self._database_session() as (engine, metadata):
            # parse header to build syntax tree
            diffs, sql = self._get_diffs_and_sql(engine, metadata, table_name, header, where_clause, body, skiprows, insert, update, delete)

            # create result object
            result = {}

            if execute:
                # execute sql statements
                results_tuple = self._execute_sql(engine, sql, commit)
                # summarize results
                # zip row counts with sql statements

            return result

    def _get_diffs_and_sql(self, table_name, header, where_clause, body, skiprows, insert, update, delete, deduplicate):
        # get cnx from context
        cnx = cnx_context.cnx

        # parse header to build syntax tree
        mapping = HeaderParser(get_metadata(cnx), table_name).parse_csv(header)

        # read dataframe from request first, so we can give feedback on errors in the request
        df_request = self._read_from_request(mapping, body, skiprows, deduplicate)

        # read dataframe from DB
        df_db = self._read_from_db(mapping, where_clause, set_index=True)

        # compare to obtain diff
        diffs = self._compare(df_request, df_db, insert, update, delete)

        # create sql statements and parameters
        sqls = self._diff_to_sql.diff_sql(mapping, diffs)

        return diffs, sqls

    def _read_from_request(self, mapping, body, skiprows, deduplicate=False):
        # get columns and unique columns.
        column_names = HeaderCompiler().compile_list(mapping, include_skip=True)
        index_columns = HeaderCompiler().compile_list_unique(mapping)
        column_types = TypesCompiler().compile(mapping, column_names, include_skip=True)

        # assert that at least one column header is not empty
        if not [c for c in column_names if c != '']:
            raise ValueError("At least one column header must not be empty")

        # read body as csv to count the number of columns in the body
        # there may be a more efficient way without having to read the csv twice.
        body_column_count = self._count_body_columns(body)

        # pad column names with empty columns if the body has more columns than the header
        if body_column_count > len(column_names):
            # add empty columns to the header
            column_names += [''] * (body_column_count - len(column_names))

        # replace empty column names with skip, skip1, skip2. This is because pandas requires column names to be unique
        non_empty_column_names = list(self._replace_empty_columns_with_skip(column_names))

        # find duplicate column names
        duplicate_column_names = self._find_duplicate_names(non_empty_column_names)

        # if there are duplicate column names, raise an exception
        if duplicate_column_names:
            raise ValueError(f"Duplicate column names are not supported: {', '.join(duplicate_column_names)}")

        # get list of columns to use in the output dataframe
        use_columns = [c for c in non_empty_column_names if c in column_names]

        # list names of columns with datetime64 or date type, because we need to parse them as datetime
        parse_dates = column_types.get('read_csv_parse_dates', {})

        # get converter dictionary
        converters = column_types.get('read_csv_converters', {})

        # get dtypes for read_csv
        dtype = column_types.get('read_csv_dtypes', {})

        # create initial column names to read the csv before padding
        initial_index_columns = [c for c in index_columns if c in non_empty_column_names[:body_column_count]]
        initial_names = non_empty_column_names[:body_column_count]
        initial_usecols = [c for c in use_columns if c in non_empty_column_names[:body_column_count]]

        # read csv from request body
        # treat '' as missing value, but treat NA as string
        df = pd.read_csv(
            StringIO(body),
            names=initial_names,
            index_col=initial_index_columns,
            skipinitialspace=True,
            skiprows=skiprows,
            dtype=dtype,
            # skip the empty column names that we renamed to skip, skip1, etc
            usecols=initial_usecols,
            parse_dates=parse_dates,
            converters=converters,
            na_values=[''],
            keep_default_na=False
        )

        df_padded = self._pad_dataframe_with_empty_columns(df, use_columns, index_columns, converters)

        # evaluate column expressions
        self._evaluate_expressions(df_padded, mapping, index_columns)

        # deduplicate if requested
        if deduplicate:
            df_padded = self._deduplicate(df_padded, index_columns)

        return df_padded

    def _evaluate_expressions(self, df, mapping, index_columns):
        # a column header can contain a python expression. Evaluate these now that we've read all values from CSV.

        # drop index, so that we can use index columns in expressions
        if index_columns:
            df.reset_index(inplace=True)

        # store original column names so we can restore them later
        original_column_names = df.columns

        # remove foreign keys and modifiers in column names so we can use the bare names in expressions
        df.columns = df.columns.str.replace(r'\[.*\]', '', regex=True)
        df.columns = df.columns.str.replace(r'\(.*\)', '', regex=True)

        # now that we have all data, we can evaluate column expressions.
        for column in mapping['columns']:
            if 'exp' in column:
                # get column name, assuming a single attribute
                column_name = column['attributes'][0]['name']

                # evaluate expression of the form <target_column>=<expression>
                expression = f"{column_name}={column['exp']}"
                df.eval(expression, inplace=True)

        # restore column names
        df.columns = original_column_names

        # restore index
        if index_columns:
            df.set_index(index_columns, inplace=True)

        # remove skip columns, because we've evaluated expressions so we no longer need them. Match column name with 'skip=true':
        df.drop(columns=[column for column in df.columns if 'skip=true' in column], errors='ignore', inplace=True)

    def _read_from_db(self, mapping, where_clause, set_index=False):
        # get sqlalchemy engine from context
        engine = cnx_context.engine

        # get enabled and unique columns and column types
        column_names = HeaderCompiler().compile_list(mapping)
        index_columns = HeaderCompiler().compile_list_unique(mapping)
        column_types = TypesCompiler().compile(mapping, column_names)

        # read dataframe from DB
        query = self._create_select_query(mapping, where_clause)
        df = pd.read_sql_query(query, engine)

        # set headers, they must equal the request headers for comparison
        df.columns = column_names

        # set index columns, if header contains unique columns
        if set_index and index_columns:
            df.set_index(index_columns, inplace=True)

        # apply converters after reading from DB, because read_sql_query() doesn't support converters
        converters = column_types['read_db_converters']
        self._apply_converters(column_names, converters, df)

        # force pandas to not convert int columns to float if they contain NaNs
        df = df.convert_dtypes()

        return df

    def _apply_converters(self, column_names, converters, df):
        # iterate the columns to apply converters one by one to either value or index column
        for column in column_names:
            if column in converters:
                if column in df.columns:

                    # apply this converter to value column
                    df[column] = df[column].apply(converters[column])
                elif column in df.index.names:

                    # get the index column
                    index_column = df.index.levels[df.index.names.index(column)]

                    # apply this converter to the index column
                    df.index = df.index.set_levels(index_column.map(converters[column]), level=column)

    def _compare(self, df_request, df_db, insert, update, delete):
        # remove columns with empty names. Don't do this when reading from DB, because in get_table request we also want empty columns
        df_db = df_db.drop(columns=[''], errors='ignore')

        # find rows to insert
        inserted_indices = set(df_request.index.values).difference(df_db.index.values)
        inserts = df_request[df_request.index.isin(inserted_indices)]
        inserts.reset_index(inplace=True)

        # find rows to delete
        deleted_indices = set(df_db.index.values).difference(df_request.index.values)
        deletes = df_db[df_db.index.isin(deleted_indices)]
        deletes.reset_index(inplace=True)

        # find rows to update
        left = df_request[~df_request.index.isin(inserted_indices)]
        right = df_db[~df_db.index.isin(deleted_indices)]

        # Sort the right DataFrame to match the index of the left DataFrame
        right_df_sorted = right.reindex(left.index)

        # get mask of cells that are N/A or '' in left and right
        mask = (left.isna() | left.eq('')) & (right_df_sorted.isna() | right_df_sorted.eq(''))

        # replace N/A or '' with None using mask to ignore differences in N/A or ''
        left = left.mask(mask, None)
        right_df_sorted = right_df_sorted.mask(mask, None)

        # compare remaining rows
        updates = left.compare(right_df_sorted)
        updates.reset_index(inplace=True)

        # return result
        return inserts if insert else DataFrame(), updates if update else DataFrame(), deletes if delete else DataFrame()

    def get_select_statement(self, table_name, header, where_clause=None):
        # get cnx from context
        cnx = cnx_context.cnx

        # parse header to build mapping
        mapping = HeaderParser(get_metadata(cnx), table_name).parse_csv(header)

        # Read data from the database table using pandas
        query = self._create_select_query(mapping, where_clause)

        return str(query)

    def _create_select_query(self, mapping, where_clause):
        # translate syntax tree to select query
        return SelectCompiler().compile(mapping, where_clause)

    def get_header_json(self, table_name, header=None):
        # get mapping, merged if needed
        mapping = self._get_header_mapping(table_name, header)

        # compile into header
        json_header = HeaderCompiler().compile_json(mapping)

        return json_header

    def get_header_csv(self, table_name, header=None):
        # get mapping, merged if needed
        mapping = self._get_header_mapping(table_name, header)

        # compile into header
        csv_header = HeaderCompiler().compile_csv(mapping)

        return csv_header

    def _get_header_mapping(self, table_name, header=None):
        # get a MetaData instance from context
        metadata = get_metadata(cnx_context.cnx)

        # create default mapping from table
        mapping = OdooHeaderParser(metadata, cnx_context.cr).parse(table_name)

        if not header:
            # return full mapping
            return mapping

        # parse header to build syntax tree
        parsed_header = HeaderParser(metadata, table_name).parse_csv(header)

        # merge headers
        merged_mapping = HeaderMerger().merge(mapping, parsed_header)

        return merged_mapping

    def _sort_queries(self, queries):
        inserts, updates, deletes = queries

        # order all queries, start with deletes
        sorted_queries = list(deletes) + list(inserts) + list(updates)
        return sorted_queries

    def _execute_sql(self, query_executors, commit=False):
        # get cursor from context
        cr = cnx_context.cr

        result = []

        # iterate query executors
        for query_executor in query_executors:
            # delegate execution to query executor
            execute_result = query_executor.execute(cr)
            # append result to list
            result.append(execute_result)

        # commit if requested
        if commit:
            cnx_context.cnx.commit()

            # registry may not be availabe during unit tests
            if hasattr(cnx_context, 'registry'):
                # invalidate caches to avoid stale values coming from cache
                cnx_context.registry.clear_cache()

        return result

    def set_context(self, url, password):
        # create psycopg2 connection
        cnx = psycopg2.connect(url, password=password)
        # set connection and cursor in context
        cnx_context.cnx = cnx
        cnx_context.cr = cnx.cursor()

    def _replace_empty_columns_with_skip(self, column_names):
        # replace empty column names with skip, skip1, skip2. This is because pandas doesn't like empty column names
        i = 0
        for name in column_names:
            if name == '':
                name = 'skip' + ('' if i == 0 else str(i))
                i += 1
            yield name

    def _find_duplicate_names(self, names):
        # find duplicate names
        seen = set()
        duplicates = set()
        for name in names:
            if name in seen:
                duplicates.add(name)
            seen.add(name)
        return duplicates

    def _get_selected_columns(self, table_name, all_columns, header):
        # get cnx from context
        cnx = cnx_context.cnx

        # parse header to build syntax tree
        mapping = HeaderParser(get_metadata(cnx), table_name).parse_csv(header)

        # reconstruct original header from tree
        column_keys = HeaderCompiler().compile_list(mapping)

        # create result list for selected and unselected columns
        result_columns = []

        # initiate list for unselected columns with all default columns
        unselected_columns = all_columns.copy()

        # iterate header columns to retain order of selected items
        for key in column_keys:

            # find column with matching key
            column = next((c for c in unselected_columns if c['key'] == key), None)

            # raise error if if column is not found,
            if column is None:
                raise ValueError(f'Column {key} not found in table {table_name}')

            # set selected is true
            column['selected'] = True

            # remove from unselected columns and add to selected columns
            unselected_columns.remove(column)
            result_columns.append(column)

        # add remaining unselected columns to selected columns
        result_columns.extend(unselected_columns)

        return result_columns

    def _count_body_columns(self, body):
        # skipinitialspace must be true, otherwise it may split on comma's in strings
        df_initial = pd.read_csv(StringIO(body), skipinitialspace=True, header=None)

        # return the number of columns in the body
        return len(df_initial.columns)

    def _pad_dataframe_with_empty_columns(self, df, column_names, index_columns, converters):
        # get current column names
        current_columns = df.columns.tolist()

        # get number of columns to pad, take columns and indices into account
        pad_count = len(column_names) - df.shape[1] - df.index.nlevels

        # if there are too few columns
        if pad_count <= 0:
            # nothing to pad
            return df

        # get columns to pad
        pad_columns = column_names[-pad_count:]

        # pad the DataFrame with empty columns
        _logger.info(f"Padding DataFrame with {pad_count} empty columns")
        current_columns += pad_columns

        # be careful not to set index columns, the columns attribute must only be set to non-index column names
        df_padded = df.reindex(columns=current_columns, fill_value=None)

        # apply converters to padded columns
        for column in pad_columns:
            if column in converters:
                df_padded[column] = df_padded[column].apply(converters[column])

        # set additional index columns
        for column in pad_columns:
            if column in index_columns:
                df_padded.set_index(column, append=True, inplace=True)

        return df_padded

    def _deduplicate(self, df, index_columns):
        # reset index
        df_reset = df.reset_index()

        # Remove duplicate rows based on the index column
        df_unique = df_reset.drop_duplicates(subset=index_columns)

        # Set the column back as the index
        df_final = df_unique.set_index(index_columns)

        return df_final

