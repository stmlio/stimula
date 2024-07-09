"""
This class assists users in creating a default mapping based on an existing data model.
It assumes an Odoo-flavored data model by excluding columns that are typically used by Odoo for internal
purposes.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from sqlalchemy import UniqueConstraint, PrimaryKeyConstraint, select, func
from sqlalchemy.dialects.postgresql import JSONB

ODOO_SYSTEM_COLUMNS = ['create_date', 'create_uid', 'write_date', 'write_uid']


class OdooHeaderParser:

    # take metadata and cursor as instance variables
    def __init__(self, metadata, cursor):
        self._metadata = metadata
        self._cr = cursor
        self._aliases = {}

    def parse(self, table_name):
        # Get the table object
        table = self._metadata.tables.get(table_name)

        if table is None:
            raise ValueError('Table not found: ' + table_name)

        return {'table': table.name, 'columns': self._columns(table)}

    def _columns(self, table):
        # get in use columns once, in a single query
        in_use_columns = self._get_in_use_columns(table)

        # get all columns
        all_columns = [self._column(c, in_use_columns) for c in table.columns]

        # We must have at least one column enabled. If no columns are enabled, then enable all columns.
        if not any([c.get('enabled', False) for c in all_columns]):
            for c in all_columns:
                c['enabled'] = True

        # Make sure we have at least one unique column
        self._enforce_unique_column(all_columns)

        # sort columns
        sorted_columns = self._sort_columns(all_columns)

        return sorted_columns

    def _enforce_unique_column(self, all_columns):
        # We must have at least one unique key. If no unique keys are enabled, then find the primary key and mark is as unique, default and enabled.
        if any([c.get('unique', False) for c in all_columns]):
            # fine, we have a unique column
            return
        for c in all_columns:
            if c.get('primary-key', False):
                c['unique'] = True
                c['default'] = True
                c['enabled'] = True

    def _column(self, column, in_use_columns):
        # a single attribute per column suffices to address a row
        attribute = {
            'name': str(column.key),
            'type': str(column.type).lower()
        }

        # create foreign key attribute
        if column.foreign_keys:
            attribute['foreign-key'] = self._foreign_key(column.foreign_keys)

        result = {'attributes': [attribute]}

        # set in-use attribute
        if column.name in in_use_columns:
            result['in-use'] = True

        # get constraints
        constraints = self._get_constraints(column)

        # set primary key and unique attributes
        for constraint in constraints:
            if constraint.__class__ == PrimaryKeyConstraint:
                result['primary-key'] = True
            elif self._is_applicable_unique_constraint(constraint):
                result['unique'] = True

        # set default attribute
        default = not result.get('primary-key', False) and not attribute['name'] in ODOO_SYSTEM_COLUMNS and result.get('in-use', False)
        if default:
            result['default'] = True
            result['enabled'] = True

        return result

    def _get_in_use_columns(self, table):
        # create list of column names
        column_names = [c.name for c in table.columns]
        # create column expressions
        expr = select(*[func.bool_or(column.isnot(None)) for column in table.columns])
        # execute query
        self._cr.execute(str(expr))
        result = self._cr.fetchone()
        # zip and filter non-null columns
        result = [c[0] for c in zip(column_names, result) if c[1]]
        # return list
        return result

    def _get_constraints(self, column):
        constraints = [constraint for constraint in column.table.constraints if column in list(constraint.columns)]
        return constraints

    def _foreign_key(self, foreign_keys):
        if len(foreign_keys) != 1:
            raise ValueError(f"Expected 1 foreign key, found {len(foreign_keys)}")
        foreign_key = list(foreign_keys)[0]
        table = foreign_key.column.table
        name = foreign_key.column.name
        return {'table': table.name, 'name': name, 'attributes': self._unique_columns(table)}

    def _unique_columns(self, table):
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

    def _unique_column(self, column):
        attribute = {'name': column.name, 'type': str(column.type).lower()}
        if column.foreign_keys:
            attribute['foreign-key'] = self._foreign_key(column.foreign_keys)

        return attribute

    def _sort_columns(self, columns):
        # get primary key columns
        primary_key_columns = [c for c in columns if c.get('primary-key', False)]

        # get unique columns, exclude primary keys
        unique_columns = [c for c in columns if not c.get('primary-key', False) and c.get('unique', False)]

        # get other columns
        other_columns = [c for c in columns if not c.get('primary-key', False) and not c.get('unique', False)]

        return sorted(primary_key_columns, key=lambda e: e['attributes'][0]['name']) \
            + sorted(unique_columns, key=lambda e: e['attributes'][0]['name']) \
            + sorted(other_columns, key=lambda e: e['attributes'][0]['name'])
