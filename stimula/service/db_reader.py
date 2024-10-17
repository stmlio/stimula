import pandas as pd

from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.header_compiler import HeaderCompiler
from stimula.compiler.select_compiler import SelectCompiler
from stimula.compiler.types_compiler import TypesCompiler
from stimula.service.context import cnx_context


class DbReader:
    def read_from_db(self, mapping, where_clause, set_index=False):
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

        # apply converters after reading from DB, because read_sql_query() doesn't support converters
        converters = column_types['read_db_converters']
        self._apply_converters(column_names, converters, df)

        # force pandas to not convert int columns to float if they contain NaNs
        df = df.convert_dtypes()

        # set index columns, if header contains unique columns. Set index after converting, because we need to convert dict to frozenset so it is hashable.
        if set_index and index_columns:
            df.set_index(index_columns, inplace=True)

        return df

    def _create_select_query(self, mapping, where_clause):
        # add aliases and parameter names
        aliased_mapping = AliasCompiler().compile(mapping)

        # translate syntax tree to select query
        return SelectCompiler().compile(aliased_mapping, where_clause)

    def _apply_converters(self, column_names, converters, df):
        # iterate the columns to apply converters one by one to either value or index column
        for column in column_names:
            if column in converters:
                if column in df.columns:

                    # apply this converter to value column
                    df[column] = df[column].apply(converters[column])
                elif column in df.index.names:
                    if isinstance(df.index, pd.MultiIndex):

                        # get the index column for a MultiIndex
                        index_column = df.index.levels[df.index.names.index(column)]

                        # apply this converter to the index column for a MultiIndex
                        df.index = df.index.set_levels(index_column.map(converters[column]), level=column)
                    else:
                        # if it's not a MultiIndex, just apply the converter to the single-level index
                        df.index = df.index.map(converters[column])
