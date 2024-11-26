import base64
import hashlib
import importlib
import logging
import os
import sys
from io import StringIO

import pandas as pd

from stimula.service.api_reader import ApiReader
from stimula.stml.header_renderer import HeaderRenderer
from stimula.stml.model import Entity, Attribute
from stimula.stml.sql.types_renderer import TypesRenderer

_logger = logging.getLogger(__name__)


class CsvReader:
    def read_from_request(self, mapping, body, skiprows, post_script=None, substitutions_map=None):

        # get columns and unique columns. Include all columns, including skip and orm-only columns.
        column_names = HeaderRenderer().render_list(mapping, include_skip=True, include_orm_only=True)
        index_columns = HeaderRenderer().render_list_unique(mapping)
        column_types = TypesRenderer().render(mapping, column_names, include_skip=True, include_orm_only=True, substitutions=substitutions_map)
        deduplicate_columns = HeaderRenderer().render_list_deduplicate(mapping)

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

        # invoke apis to retrieve additional data, such as attachments
        self._invoke_apis(df_padded, mapping, index_columns)

        # evaluate column expressions, must do after restoring index and column types
        self._evaluate_expressions(df_padded, mapping, column_names, index_columns)

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
                # a jsonb column has type object, but pandas does not support setting a multi-index to type object
                if not type == 'object':
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

    def _invoke_apis(self, df, mapping: Entity, index_columns):
        # a column header can contain an 'api' modifier. Invoke these now that we've read all values from CSV, but before evaluating expressions.

        # drop index, so that we can use index columns in expressions
        if index_columns:
            df.reset_index(inplace=True)

        # store original column names so we can restore them later
        original_column_names = df.columns

        # remove foreign keys and modifiers in column names so we can use the bare names in expressions
        df.columns = df.columns.str.replace(r'\[.*\]', '', regex=True)
        df.columns = df.columns.str.replace(r'\(.*\)', '', regex=True)

        # now that we have all data, we can evaluate api calls.
        for attribute in mapping.attributes:
            if isinstance(attribute, Attribute) and attribute.api:
                # get column name, assuming a single attribute
                column_name = attribute.name

                # assert that the column has a url modifier
                assert attribute.url, f"Column {column_name} has an 'api' modifier, but no 'url' modifier"

                # drop all values in the column and set type to binary
                df[column_name] = None
                df[column_name] = df[column_name].astype('bytes')

                # iterate rows and invoke the api
                for index, row in df.iterrows():
                    # read document and add the binary response to the row in the DataFrame
                    df.at[index, column_name] = ApiReader().read_document(attribute.url, row.to_dict())

        # restore column names
        df.columns = original_column_names

        # restore index
        if index_columns:
            df.set_index(index_columns, inplace=True)

    def _evaluate_expressions(self, df, mapping, column_names, index_columns):
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
        for attribute in mapping.attributes:
            if attribute and attribute.exp:
                # get column name, assuming a single attribute
                column_name = attribute.name

                # evaluate expression of the form <target_column>=<expression>
                expression = f"{column_name}={attribute.exp}"

                # evaluate the expression, pass custom functions
                df.eval(expression, inplace=True, local_dict={'checksum': checksum, 'base64encode': base64encode, 'concat': concat, 'fallback': fallback})

        # restore column names
        df.columns = original_column_names

        # restore index
        if index_columns:
            df.set_index(index_columns, inplace=True)

        # list all columns with 'skip=true' in their mapping, but not API results. Assume single attribute.
        drop_column_names = [n for n, a in zip(column_names, mapping.attributes) if a and a.skip and not a.api]

        # drop these columns, because we've evaluated expressions so we no longer need them. But keep API results, we'll use them later.
        df.drop(columns=drop_column_names, errors='ignore', inplace=True)

    def _deduplicate(self, df, index_columns_to_deduplicate):
        # get original index columns
        original_index_columns = df.index.names

        # reset index
        df_reset = df.reset_index()

        # Remove duplicate rows based on the index column
        df_unique = df_reset.drop_duplicates(subset=index_columns_to_deduplicate)

        # Set the column back as the index
        df_final = df_unique.set_index(original_index_columns)

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


def checksum(series):
    # checksum function for custom expression. Return hex digest for all items in series an return as type string.
    return series.apply(_hexdigest).astype('string')


def _hexdigest(x):
    # if x is None, return None
    if pd.isna(x):
        return None
    # if x is str, encode and return hex digest
    if isinstance(x, str):
        return hashlib.sha1(x.encode()).hexdigest()
    # if x is bytes, return hex digest
    if isinstance(x, bytes):
        return hashlib.sha1(x).hexdigest()
    # else raise an exception
    raise ValueError(f"Unsupported type {type(x)}")


def base64encode(series):
    # UU-encode function for custom expression.
    return series.apply(_base64encode).astype('string')


def _base64encode(x: str | bytes | None) -> str | None:
    # Return None if the input is None
    if x is None:
        return None

    # If the input is a string, encode it to bytes and Base64-encode
    if isinstance(x, str):
        return base64.b64encode(x.encode()).decode('utf-8')

    # If the input is bytes, Base64-encode it directly
    if isinstance(x, bytes):
        return base64.b64encode(x).decode('utf-8')

    # Raise an exception for unsupported types
    raise ValueError(f"Unsupported type {type(x)}")


def concat(*series):
    # concat function for custom expression. Return concatenated string for all items in series. Return as type 'string'.
    sep = series[0] or ''
    enc = '"' if bool(series[1]) else ''
    return pd.Series(list(zip(*series[2:]))).apply(lambda s: _concat(sep, enc, s)).astype('string')


# a concat function that accepts 1+ arguments and returns a concatenated string, using first parameter as separator. If second parameter is true, then enclose in quotes
def _concat(sep, enc, series):
    # concatenate non-empty items in series, using sep as separator and dec as enclosure
    return sep.join(enc + str(s) + enc for s in series if not pd.isna(s) and not s == '') or ''


def fallback(*series):
    # takes any number of values and returns the first non-null and non-empty value
    return pd.Series(list(zip(*series))).apply(_fallback).astype('string')


def _fallback(args):
    for arg in args:
        if not pd.isna(arg) and not arg == '':
            return arg
    return ''
