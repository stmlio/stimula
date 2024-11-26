"""
This class provides the main database service to list tables, create mappings, retrieve table contents and to post updates.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import logging
import re
from io import StringIO
from typing import Optional

import pandas as pd
import psycopg2
from pandas import DataFrame

from .abstract_orm import AbstractORM
from .context import cnx_context, get_metadata
from .csv_reader import CsvReader
from .db_reader import DbReader
from .diff_to_executor import DiffToExecutor
from .executor_service import ExecutorService
from .odoo.postgres_model_service import PostgresModelService
from .query_executor import OperationType
from .reporter import Reporter
from ..stml.header_renderer import HeaderRenderer
from ..stml.json_renderer import JsonRenderer
from ..stml.model import Entity
from ..stml.model_enricher import ModelEnricher
from ..stml.parameter_expander import ParameterExpander
from ..stml.sql.select_renderer import SelectRenderer
from ..stml.sql.types_renderer import TypesRenderer
from ..stml.stml_creator import StmlCreator
from ..stml.stml_merger import StmlMerger
from ..stml.stml_parser import StmlParser

_logger = logging.getLogger(__name__)


class DB:
    def __init__(self, orm_function=None):
        self._diff_to_sql = DiffToExecutor()
        # delay orm creation until needed
        self._orm_function = orm_function

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
            default_mapping = StmlCreator(PostgresModelService()).create(table_name)

            # remove columns that are not enabled
            attributes = [a for a in default_mapping.attributes if a.enabled]
            mapping = Entity(table_name, attributes)

        else:
            # parse header to build syntax tree
            mapping = ModelEnricher(metadata).enrich(StmlParser().parse_csv(table_name, header))

        query = SelectRenderer().compile_count_query(mapping, where_clause)

        cr = cnx_context.cr

        # execute query
        cr.execute(query)

        # fetch result
        result = cr.fetchone()

        return result[0]

    def get_table(self, table_name, header, where_clause=None, protocol='sql'):
        # create metadata object from context
        metadata = get_metadata(cnx_context.cnx)

        if not header:
            # if header parameter is empty, then use the default header
            default_mapping = StmlCreator(PostgresModelService()).create(table_name)

            # remove columns that are not enabled
            attributes = [a for a in default_mapping.attributes if a.enabled]
            mapping = Entity(table_name, attributes)

        else:
            # parse header to build syntax tree
            mapping = ModelEnricher(PostgresModelService()).enrich(StmlParser().parse_csv(table_name, header))

        df = DbReader(protocol).read_from_db(mapping, where_clause)

        return df, mapping

    def get_table_as_csv(self, table_name, header, where_clause, escapechar=None):
        # get table as dataframe
        df, mapping = self.get_table(table_name, header, where_clause)

        return self.convert_to_csv(df, mapping, escapechar)

    def convert_to_csv(self, df, mapping, escapechar=None):
        # need converters from types compiler
        column_names = HeaderRenderer().render_list(mapping)
        column_types = TypesRenderer().render(mapping, column_names)

        # get converters from column_types
        converters = column_types['write_csv_converters']

        # apply converters to dataframe
        for column in column_names:
            if column in converters:
                df[column] = df[column].apply(converters[column])

        # convert dataframe to csv
        return df.to_csv(index=False, escapechar=escapechar)

    def post_table_get_diff(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False, post_script=None, context=None, orm=None):
        # create diffs and sql
        diffs, sql = self._get_diffs_and_sql(table_name, header, where_clause, body, skiprows, insert, update, delete, post_script, context, orm)
        # if execute
        if execute:
            # execute sql statements
            results_tuple = self._execute_sql(sql, True, commit)
            # concatenate results
            results = [result for results in results_tuple for result in results]
            # zip row counts with diffs statements
            for diff, result in zip(diffs, results):
                diff.append(result.rowcount)

        return diffs

    def post_table_get_sql(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False, post_script=None, context=None):
        # create diffs and sql
        _, query_executors = self._get_diffs_and_sql(table_name, header, where_clause, body, skiprows, insert, update, delete, post_script, context)

        # execute sql statements, or fake if execute is false
        sqls = ExecutorService().execute_sql(query_executors, execute, commit)

        # convert sql to dataframe
        return self._convert_to_df(sqls, execute)

    def post_table_get_full_report(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False,
                                   post_script=None, context=None, substitutions=None):

        # create orm service if function is provided
        orm: Optional[AbstractORM] = self._orm_function() if self._orm_function else None

        # create diffs and sql
        diff, query_executors = self._get_diffs_and_sql(table_name, header, where_clause, body, skiprows, insert, update, delete, post_script, context, orm=orm, substitutions=substitutions)

        # execute sql statements
        execution_results = ExecutorService().execute_sql(query_executors, execute, commit)

        # create full report
        return Reporter().create_post_report([table_name], [body], [context], execution_results, execute, commit, skiprows)

    def post_multiple_tables_get_full_report(self, table_names, header, where_clause, contents, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False,
                                             post_script=None, context=None, substitutions=None):
        assert len(table_names) == len(contents), f"Provide exactly one file for each table name, so {len(table_names)}, not {len(contents)}"
        assert header is None, "Header must be None when posting multiple tables"
        assert skiprows == 1, "Skiprows must be 1 when posting multiple tables"
        assert post_script is None, "Post script must be None when posting multiple tables"
        assert context is not None and len(context) == len(table_names), "Provide exactly one context for each table name, not %s" % len(context or [])

        query_executors = []

        # create orm service if function is provided
        orm: Optional[AbstractORM] = self._orm_function() if self._orm_function else None

        # Iterate over tables here.
        for table_name, file_context, content in zip(table_names, context, contents):
            # decode binary content
            text_content = content.decode('utf-8')
            text_substitutions = substitutions.decode('utf-8') if substitutions else None

            # get header from first line
            header = text_content.split('\n', 1)[0]

            # create diffs and sql
            _, qe = self._get_diffs_and_sql(table_name, header, where_clause, text_content, skiprows, insert, update, delete, post_script, file_context, orm=orm, substitutions=text_substitutions)
            query_executors.extend(qe)

        # execute sql statements
        execution_results = ExecutorService().execute_sql(query_executors, execute, commit)

        # create full report
        return Reporter().create_post_report(table_names, contents, context, execution_results, execute, commit, skiprows)

    def _convert_to_df(self, sqls, showResult):
        # create empty pandas dataframe. First column contains sql
        if showResult:
            result = pd.DataFrame(columns=['rows', 'errors', 'sql'])
        else:
            result = pd.DataFrame(columns=['sql'])

        # iterate sql statements
        index = 0
        for er in sqls:
            # create dictionary with query and value_dict
            value_dict = {**er.params, 'sql': er.query}
            if showResult:
                value_dict = {**value_dict, 'rows': er.rowcount, 'errors': er.error}
            # append a new row to the bottom result with values from value dictionary using concat
            result = pd.concat([result, pd.DataFrame(value_dict, index=[index])], ignore_index=True)
            index += 1

            # append dependent query result
            dep_er = er.dependent_execution_result
            if dep_er:
                value_dict = {**dep_er.params, 'sql': dep_er.query}
                if showResult:
                    value_dict = {**value_dict, 'rows': dep_er.rowcount}
                result = pd.concat([result, pd.DataFrame(value_dict, index=[index])], ignore_index=True)
                index += 1

        # force pandas to not convert int columns to float if they contain NaNs
        result = result.convert_dtypes()

        # return dataframe
        return result

    def post_table_get_summary(self, table_name, header, where_clause, body, skiprows=0, insert=False, update=False, delete=False, execute=False, commit=False):
        pass

    def _get_diffs_and_sql(self, table_name, header, where_clause, body, skiprows, insert, update, delete, post_script, context, orm: Optional[AbstractORM] = None, substitutions: str=None):

        # if header is empty and skiprows is 1, then take the first line as header
        if not header and skiprows == 1:
            # get header from first line
            header = body.split('\n', 1)[0]

        # assert that we have a header
        assert header, "Header is required, either as a parameter or as the first line in the body with skiprows set to 1"

        # parse header to build syntax tree
        mapping = ModelEnricher(PostgresModelService()).enrich(StmlParser().parse_csv(table_name, header))

        # prepare substitutions map
        substitutions_map = self._create_substitutions_map(substitutions)

        # expand parameter placeholders
        mappings = ParameterExpander().expand(mapping, substitutions_map)

        diffs = None
        sqls = []

        # iterate over mappings
        for mapping in mappings:

            # read dataframe from request first, so we can give feedback on errors in the request
            df_request = CsvReader().read_from_request(mapping, body, skiprows, post_script, substitutions_map)

            # read dataframe from DB
            df_db = DbReader().read_from_db(mapping, where_clause, set_index=True)

            # todo: remove the need to return diffs
            diffs = self._compare(df_request, df_db, insert, update, delete)

            # create sql statements and parameters
            sqls.extend(self._diff_to_sql.diff_executor(mapping, diffs, context, orm))

        return diffs, sqls

    def _compare(self, df_request, df_db, insert, update, delete):
        # remove columns with empty names. Don't do this when reading from DB, because in get_table request we also want empty columns
        df_db = df_db.drop(columns=[''], errors='ignore')

        # find rows to insert
        inserted_indices = set(df_request.index.values).difference(df_db.index.values)
        inserts = df_request[df_request.index.isin(inserted_indices)]
        inserts.reset_index(inplace=True)

        # move __line__ to become the left most column after index reset
        inserts = self._move_line_to_front(inserts)

        # find rows to delete
        deleted_indices = set(df_db.index.values).difference(df_request.index.values)
        deletes = df_db[df_db.index.isin(deleted_indices)]
        deletes.reset_index(inplace=True)

        # find rows to update. Left is from request, right is from database
        left = df_request[~df_request.index.isin(inserted_indices)]
        right = df_db[~df_db.index.isin(deleted_indices)]

        # Sort the right DataFrame to match the index of the left DataFrame
        right_df_sorted = right.reindex(left.index)

        # get columns that are in left but not in right, such as __line__ and API filled columns
        drop_column_names = set(left.columns).difference(right_df_sorted.columns)

        # drop these columns from left
        left_no_line = left.drop(columns=drop_column_names, errors='ignore')

        # get mask of cells that are N/A or '' in left and right
        mask = (left_no_line.isna() | left_no_line.eq('')) & (right_df_sorted.isna() | right_df_sorted.eq(''))

        # replace N/A or '' with None using mask to ignore differences in N/A or ''
        left_no_line = left_no_line.mask(mask, None)
        right_df_sorted = right_df_sorted.mask(mask, None)

        # compare remaining rows
        updates = left_no_line.compare(right_df_sorted)

        # restore dropped columns from left into updates
        for column_name in drop_column_names:
            updates[column_name] = left.loc[updates.index][column_name]

        # reset index for updates to match the insert and deletes
        updates.reset_index(inplace=True)

        # move __line__ to become the left most column
        updates = self._move_line_to_front(updates)

        # return result
        return inserts if insert else DataFrame(), updates if update else DataFrame(), deletes if delete else DataFrame()

    def _move_line_to_front(self, df):
        # move __line__ to become the left most column. This has no real purpose, but it makes the dataframes more readable
        # this must also work with the multi-index dataframes coming from the compare function
        # pop __line__ column, don't use drop because that triggers a warning
        line = df.pop('__line__')
        # insert __line__ column as first column
        df.insert(0, '__line__', line)
        return df

    def get_header_json(self, table_name, header=None):
        # get mapping, merged if needed
        mapping = self._get_header_mapping(table_name, header)

        # compile into header
        json_header = JsonRenderer().render_json(mapping)

        return json_header

    def get_header_csv(self, table_name, header=None):
        # get mapping, merged if needed
        mapping = self._get_header_mapping(table_name, header)

        # compile into header
        csv_header = HeaderRenderer().render_csv(mapping)

        return csv_header

    def _get_header_mapping(self, table_name, header=None):
        # get a MetaData instance from context
        metadata = get_metadata(cnx_context.cnx)

        # create default mapping from table
        mapping = StmlCreator(PostgresModelService()).create(table_name)

        if not header:
            # return full mapping
            return mapping

        # parse header to build syntax tree
        parsed_header = ModelEnricher(PostgresModelService()).enrich(StmlParser().parse_csv(table_name, header))

        # merge headers
        merged_mapping = StmlMerger().merge(mapping, parsed_header)

        return merged_mapping



    def set_context(self, url, password):
        # create psycopg2 connection
        cnx = psycopg2.connect(url, password=password)
        # set connection and cursor in context
        cnx_context.cnx = cnx
        cnx_context.cr = cnx.cursor()


    def _get_selected_columns(self, table_name, all_columns, header):
        # get cnx from context
        cnx = cnx_context.cnx

        # parse header to build syntax tree
        mapping = ModelEnricher(PostgresModelService()).enrich(StmlParser().parse_csv(table_name, header))

        # reconstruct original header from tree
        column_keys = HeaderRenderer().render_list(mapping)

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

    def _create_substitutions_map(self, substitutions):
        # convert substitutions table into a dictionary
        if not substitutions:
            # return None to indicate no substitutions were provided
            return None

        # read substitutions from binary string into a DataFrame
        df_substitutions = pd.read_csv(StringIO(substitutions), dtype=str, na_filter=False)

        # convert to dictionary, first column is the domain
        map = {row.strip().lower(): {} for row in df_substitutions.iloc[:, 0].unique()}

        # iterate rows, second column is the name, third column is the substitution for the name
        for row in df_substitutions.itertuples(index=False):
            map[row[0].strip().lower()][row[1].strip().lower()] = row[2].strip()

        return map
