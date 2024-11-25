"""
This class assists users in creating a default mapping based on an existing data model.
It assumes an Odoo-flavored data model by excluding columns that are typically used by Odoo for internal
purposes.

Author: Romke Jonker
Email: romke@stml.io
"""
from typing import List

from sqlalchemy import UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB

from stimula.service.model_service import ModelService
from stimula.stml.model import Entity, Attribute, Reference, AbstractAttribute

ODOO_SYSTEM_COLUMNS = ['create_date', 'create_uid', 'write_date', 'write_uid']


class StmlCreator:

    # take metadata and cursor as instance variables
    def __init__(self, model_service: ModelService):
        self._model_service: ModelService = model_service

    def create(self, table_name):
        # Get the table object
        table = self._model_service.get_table(table_name)

        if table is None:
            raise ValueError('Table not found: ' + table_name)

        return Entity(table.name, self._attributes(table))

    def _attributes(self, table):
        # get in use columns once, in a single query
        in_use_columns = self._model_service.get_non_empty_columns(table)

        # get all columns
        attributes = [self._attribute(c, in_use_columns) for c in table.columns]

        # We must have at least one column enabled. If no columns are enabled, then enable all columns.
        if not any([a.enabled for a in attributes]):
            for a in attributes:
                a.enabled = True

        # Make sure we have at least one unique column
        self._enforce_unique_column(attributes)

        # sort columns
        sorted_columns = self._sort_columns(attributes)

        return sorted_columns

    def _enforce_unique_column(self, attributes: List[AbstractAttribute]):
        # We must have at least one unique key. If no unique keys are enabled, then find the primary key and mark is as unique, default and enabled.
        if any([a.unique for a in attributes]):
            # fine, we have a unique column
            return
        for a in attributes:
            if a.primary_key:
                a.unique = True
                a.default = True
                a.enabled = True

    def _attribute(self, column, in_use_columns):
        if not column.foreign_keys:
            # create attribute
            attribute = Attribute(column.name, type=str(column.type).lower())
        else:
            # create foreign key attribute
            attribute = self._foreign_key(column)

        # set in-use attribute
        if column.name in in_use_columns:
            attribute.in_use = True

        # get constraints
        constraints = self._get_constraints(column)

        # set primary key and unique attributes
        for constraint in constraints:
            if constraint.__class__ == PrimaryKeyConstraint:
                attribute.primary_key = True
            elif self._is_applicable_unique_constraint(constraint):
                attribute.unique = True

        # set default attribute
        default = not attribute.primary_key and not attribute.name in ODOO_SYSTEM_COLUMNS and attribute.in_use
        if default:
            attribute.default = True
            attribute.enabled = True

        return attribute

    def _get_constraints(self, column):
        constraints = [constraint for constraint in column.table.constraints if column in list(constraint.columns)]
        return constraints

    def _foreign_key(self, column) -> Reference:
        foreign_keys = column.foreign_keys
        if len(foreign_keys) != 1:
            raise ValueError(f"Expected 1 foreign key, found {len(foreign_keys)}")
        foreign_key = list(foreign_keys)[0]
        table = foreign_key.column.table
        target_name = foreign_key.column.name
        return Reference(column.name, table=table.name, target_name=target_name, attributes=self._unique_columns(table))

    def _unique_columns(self, table) -> List[AbstractAttribute]:
        # find applicable constraints
        unique_constraints = [c for c in table.constraints if self._is_applicable_unique_constraint(c)]
        primary_key_constraints = [c for c in table.constraints if c.__class__ == PrimaryKeyConstraint]

        if len(unique_constraints) > 0:
            # use unique columns from first unique constraint
            columns = unique_constraints[0].columns
        elif len(primary_key_constraints) > 0:
            # fall back on primary key columns
            columns = primary_key_constraints[0].columns
        else:
            columns = []

        return [self._unique_column(c) for c in columns]

    def _is_applicable_unique_constraint(self, c):
        # must be a unique constraint
        if c.__class__ != UniqueConstraint:
            return False

        # don't set jsonb columns as unique, because the resulting dict is not hashable
        if any([isinstance(column.type, JSONB) for column in c.columns]):
            return False

        # else it's applicable
        return True

    def _unique_column(self, column) -> AbstractAttribute:
        if not column.foreign_keys:
            return Attribute(column.name, type=str(column.type).lower())
        else:
            return self._foreign_key(column)

    def _sort_columns(self, attributes: List[AbstractAttribute]) -> List[AbstractAttribute]:
        # get primary key columns
        primary_key_columns = [a for a in attributes if a.primary_key]

        # get unique columns, exclude primary keys
        unique_columns = [a for a in attributes if not a.primary_key and a.unique]

        # get other columns
        other_columns = [a for a in attributes if not a.primary_key and not a.unique]

        return sorted(primary_key_columns, key=lambda a: a.name) \
            + sorted(unique_columns, key=lambda a: a.name) \
            + sorted(other_columns, key=lambda a: a.name)
