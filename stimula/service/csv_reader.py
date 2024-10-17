import importlib
import logging
import os
import sys
from io import StringIO

import pandas as pd

from stimula.compiler.header_compiler import HeaderCompiler
from stimula.compiler.types_compiler import TypesCompiler

_logger = logging.getLogger(__name__)


class CsvReader:
    def read_from_request(self, mapping, body, skiprows, post_script=None):
        # get columns and unique columns.
        column_names = HeaderCompiler().compile_list(mapping, include_skip=True)
        index_columns = HeaderCompiler().compile_list_unique(mapping)
        column_types = TypesCompiler().compile(mapping, column_names, include_skip=True)
        deduplicate_columns = HeaderCompiler().compile_list_deduplicate(mapping)

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
            # skip the empty column names that we renamed to skip, skip1, etc
            usecols=initial_usecols,
            parse_dates=parse_dates,
            converters=converters,
            na_values=[''],
            keep_default_na=False
        )

        # Restore index and column types, because pd set converter results to type object.
        self._restore_column_types(df, dtype)

        # pad dataframe with empty columns if we have more column names in use_columns than exist in the dataframe
        df_padded = self._pad_dataframe_with_empty_columns(df, use_columns, index_columns, converters)

        # evaluate column expressions, must do after restoring index and column types
        self._evaluate_expressions(df_padded, mapping, index_columns)

        # insert a column with line numbers
        df_padded.insert(0, '__line__', range(0, len(df)))

        # deduplicate if requested
        if deduplicate_columns:
            df_padded = self._deduplicate(df_padded, deduplicate_columns)

        # verify that there are no duplicate index values
        if df_padded.index.has_duplicates:
            # find duplicate index values
            duplicates = df_padded.index[df_padded.index.duplicated()]
            # convert to name-value dict
            duplicate_map = {k: v for k, v in zip(duplicates.names, duplicates.values)}

            raise ValueError(f"Duplicates found: {duplicate_map}")

        # apply post script if provided
        if post_script:
            # execute post script
            self._execute_post_script(df_padded, post_script)

        return df_padded

    def _count_body_columns(self, body):
        # skipinitialspace must be true, otherwise it may split on comma's in strings
        df_initial = pd.read_csv(StringIO(body), skipinitialspace=True, header=None)

        # return the number of columns in the body
        return len(df_initial.columns)

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

    def _restore_column_types(self, df, dtype):
        # iterate columns types and convert to the correct type.
        # Need to do this after reading, because pd sets converter results to type object
        # Need to do this before evaluating expressions, because numexpr (if installed) can't deal with type 'object'
        for column, type in dtype.items():
            # skip setting index types
            if column in df.columns:
                # convert column to the correct type
                df[column] = df[column].astype(type)
            elif column in df.index.names and isinstance(df.index, pd.MultiIndex):
                # convert multi-index to the correct type
                df.index = df.index.set_levels(df.index.levels[df.index.names.index(column)].astype(type), level=column)
            elif column in df.index.names and not isinstance(df.index, pd.MultiIndex):
                # convert single-level index to the correct type
                df.index = df.index.astype(type)

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

    def _deduplicate(self, df, index_columns):
        # reset index
        df_reset = df.reset_index()

        # Remove duplicate rows based on the index column
        df_unique = df_reset.drop_duplicates(subset=index_columns)

        # Set the column back as the index
        df_final = df_unique.set_index(index_columns)

        return df_final

    def _execute_post_script(self, df, post_script):
        # skip if no post script provided
        if post_script is None:
            return df

        # assert that post_script file exists
        assert os.path.exists(post_script), f"Post script file {post_script} not found"

        # import the post script module
        module_name = 'post_script'
        spec = importlib.util.spec_from_file_location(module_name, post_script)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # verify that the module has an execute function
        assert hasattr(module, 'execute'), f"Post script module {post_script} must have an execute function"

        # execute the post script
        return module.execute(df)
