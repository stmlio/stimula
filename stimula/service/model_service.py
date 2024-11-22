"""
This abstract class declares methods to access the database model.

Author: Romke Jonker
Email: romke@stml.io
"""

from abc import ABC, abstractmethod


class ModelService(ABC):

    @abstractmethod
    def get_table(self, table_name):
        pass

    @abstractmethod
    def find_primary_keys(self, table):
        pass

    @abstractmethod
    def resolve_foreign_key_table(self, table, column_name):
        pass

    @abstractmethod
    def get_non_empty_columns(self, table):
        pass

    @abstractmethod
    def read_table(self, mapping: dict, where_clause=None):
        pass
