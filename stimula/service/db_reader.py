import pandas as pd

from stimula.service.model_service import ModelService
from stimula.service.odoo.jsonrpc_model_service import JsonRpcModelService
from stimula.service.odoo.postgres_model_service import PostgresModelService
from stimula.stml.header_renderer import HeaderRenderer
from stimula.stml.sql.types_renderer import TypesRenderer

MODEL_SERVICES = {
    "sql": PostgresModelService,
    "jsonrpc": JsonRpcModelService
}


class DbReader:
    def __init__(self, protocol='sql'):
        assert protocol in MODEL_SERVICES, f"Protocol '{protocol}' not supported"
        self._model_service: ModelService = MODEL_SERVICES[protocol]()

    def read_from_db(self, mapping, where_clause, set_index=False):

        # get enabled and unique columns and column types
        column_names = HeaderRenderer().render_list(mapping)
        index_columns = HeaderRenderer().render_list_unique(mapping)
        column_types = TypesRenderer().render(mapping, column_names)

        # read dataframe from DB
        df = self._model_service.read_table(mapping, where_clause)

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
