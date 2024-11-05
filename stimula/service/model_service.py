"""
This abstract class declares methods to access the database model.

Author: Romke Jonker
Email: romke@stml.io
"""

from abc import ABC, abstractmethod


class ModelService(ABC):

    @abstractmethod
    def find_table(self, table_name):
        pass

    @abstractmethod
    def find_primary_keys(self, table):
        pass

    @abstractmethod
    def _resolve_foreign_key_table(self, table, column_name):
        pass