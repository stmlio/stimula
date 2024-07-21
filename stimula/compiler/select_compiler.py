"""
This class compiles a mapping into a select query.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import re

from .alias_compiler import AliasCompiler


class SelectCompiler:
    """
        select c.c1, c0.c1, b.b1, a.a1
        from c
        left join c as c0 on c.c2 = c0.c0
        left join b on c.c3 = b.b0
        left join b as b0 on c.c4 = b0.b0
        left join a on b0.b2=a.a0
        order by c.c1
    """

    def compile(self, mapping, where_clause=None):
        """
        Compiles a mapping into a select query
        :param mapping: the mapping
        :param where_clause: a free where clause for the caller to specify
        :return: the select query
        """
        table = mapping['table']
        aliased_mapping = AliasCompiler().compile(mapping)
        select_clause = SelectClauseCompiler().compile(aliased_mapping)
        join_clause = JoinClauseCompiler().compile(aliased_mapping)
        order_by_clause = OrderByClauseCompiler().compile(aliased_mapping)

        # compile [filter="...$..."] headers into a where clause, replacing '$' with the column name
        where = WhereClauseCompiler().compile(aliased_mapping, where_clause)
        return f'{select_clause} from {table}{join_clause}{where}{order_by_clause}'

    def compile_count_query(self, mapping, where_clause=None):
        table = mapping['table']
        aliased_mapping = AliasCompiler().compile(mapping)
        select_clause = 'select count(*)'
        join_clause = JoinClauseCompiler().compile(aliased_mapping)
        where = f' where {where_clause}' if where_clause else ''
        return f'{select_clause} from {table}{join_clause}{where}'

    def get_model_name(self, alias):
        # this method gets the model name to use in the extension clause
        # current solution is based on the alias, which is not very robust
        # alias is of the form 'xxx_xxx_xxx_d', we need to convert that into 'xxx.xxx.xxx'

        # remove '_0' postfix from alias using regex if it exists
        alias = re.sub(r'_\d+$', '', alias)
        return alias.replace('_', '.')



class SelectClauseCompiler:
    def compile(self, mapping):
        table_name = mapping['table']
        # comma separate cells. Skip cells with skip=true modifier. We need those when reading CSV, but not when reading from DB
        return 'select ' + ', '.join([self._column(c, table_name) for c in mapping['columns'] if not c.get('skip')])

    def _column(self, column, table_name):
        # column may be empty
        if not column:
            return '\'\''

        attributes = column['attributes']

        # empty column, select empty string
        if not attributes:
            return '\'\''

        # get the attributes for this column
        selects = self._attributes(attributes, table_name)

        # if there's more than one attribute, we must cast jsonb columns to string
        cast_selects = self._cast_selects_if_needed(selects)

        # colon separate attributes
        return ' || \':\' || '.join(cast_selects)

    def _attributes(self, attributes, alias):
        attribute_lists = [self._attribute(attribute, alias) for attribute in attributes]
        # flatten list of list into a list
        return [attribute for attribute_list in attribute_lists for attribute in attribute_list]

    def _attribute(self, attribute, alias):
        # if this is not a foreign key, return alias and column name
        if not 'foreign-key' in attribute:
            column_name = attribute['name']
            # also return the source attribute, so we can check the type later
            return [(f'{alias}.{column_name}', attribute)]
        else:
            foreign_key = attribute['foreign-key']
            # but table names may need an alias
            attributes = foreign_key['attributes']
            target_name = foreign_key['table']
            target_alias = foreign_key.get('alias', target_name)
            # recurse into foreign key table to get the attributes
            return self._attributes(attributes, target_alias)

    def _cast_selects_if_needed(self, selects):
        # postgres can't concatenate jsonb columns to anything, so cast to string
        cast_selects = []
        for select in selects:
            # select is a tuple of (column, attribute)
            if len(selects) > 1 and select[1].get('type') == 'jsonb':
                cast_selects.append(f'{select[0]}::text')
            else:
                cast_selects.append(select[0])
        return cast_selects


class JoinClauseCompiler:
    def compile(self, mapping):
        table_name = mapping['table']
        # glue cells together
        return ''.join([self._column(c, table_name) for c in mapping['columns']])

    def _column(self, column, table_name):
        # column may be empty
        if not column:
            return ''
        return self._attributes(column['attributes'], table_name)

    def _attributes(self, attributes, alias):
        attribute_list = [self._attribute(attribute, alias) for attribute in attributes]
        return ''.join(attribute_list)

    def _attribute(self, attribute, alias):
        # if no foreign key, return empty string
        if not 'foreign-key' in attribute:
            return ''
        else:
            foreign_key = attribute['foreign-key']
            # but table names may need an alias
            source_name = attribute['name']
            target_name = foreign_key['name']
            target_table = foreign_key['table']
            target_alias = foreign_key.get('alias', target_table)

            # Assume we need a left join. This may require a modifier at some point.
            join_clause = f' left join {target_table}'

            # only add 'as alias' if it's different from table name
            if target_alias != target_table:
                join_clause += f' as {target_alias}'
            join_clause += f' on {alias}.{source_name} = {target_alias}.{target_name}'

            # if this is an Odoo style extension relation, then we need to filter by qualifier (module) and table (model)
            if 'qualifier' in foreign_key:
                qualifier = foreign_key['qualifier']
                # assume for now that alias is the table name. This is fine as long as we're not joining the same table multiple times
                table_name = SelectCompiler().get_model_name(alias)
                join_clause += f' and {target_alias}.model = \'{table_name}\' and {target_alias}.module = \'{qualifier}\''

            attributes = foreign_key['attributes']
            return join_clause + self._attributes(attributes, target_alias)


class OrderByClauseCompiler:
    def compile(self, mapping):
        table_name = mapping['table']
        # clauses to order by
        clause_lists = [self._column(c, table_name) for c in mapping['columns']]
        clauses = [clause for clause_list in clause_lists for clause in clause_list]

        # nothing to order by
        if not clauses:
            return ''

        return ' order by ' + ', '.join(clauses)

    def _column(self, column, table_name):
        unique = column.get('unique', False)
        if not unique:
            return []

        return self._attributes(column['attributes'], table_name)

    def _attributes(self, attributes, alias):

        attribute_list = [self._attribute(attribute, alias) for attribute in attributes]

        return attribute_list

    def _attribute(self, attribute, alias):
        # if no foreign key, get value from parameter
        if not 'foreign-key' in attribute:
            column_name = attribute['name']
            return f'{alias}.{column_name}'
        else:
            foreign_key = attribute['foreign-key']
            # but table names may need an alias
            attributes = foreign_key['attributes']
            target_name = foreign_key['table']
            target_alias = foreign_key.get('alias', target_name)
            return ', '.join(self._attributes(attributes, target_alias))

class WhereClauseCompiler:
    def compile(self, mapping, where_clause):
        """
        Compiles [select="...$..."] modifiers into a where clause
        :param mapping:
        :param where_clause: additional where clause to append
        :return: the where clause
        """
        table_name = mapping['table']

        # get conditions from columns with filter modifiers
        conditions = [self._column(c, table_name) for c in mapping['columns'] if c.get('filter')]

        # append user-defined where clause
        if where_clause:
            conditions.append(where_clause)

        if not conditions:
            return ''

        # return where clause with conditions joined by ' and '
        return ' where ' + ' and '.join(conditions)

    def _column(self, column, table_name):
        # column may be empty, have no attributes and/or have no filter
        if not column or not column.get('attributes') or not column.get('filter'):
            return []

        attributes = column['attributes']

        # get the attributes for this column
        attributes_names = self._attributes(attributes, table_name)

        # a filter column must have exactly one attribute
        assert len(attributes_names) == 1, f'Filtered column {attributes} must have exactly one attribute, got {len(attributes_names)}'

        # replace $ placeholder with attribute name
        return column['filter'].replace('$', attributes_names[0])

    def _attributes(self, attributes, alias):
        attribute_lists = [self._attribute(attribute, alias) for attribute in attributes]
        # flatten list of list into a list
        return [attribute for attribute_list in attribute_lists for attribute in attribute_list]

    def _attribute(self, attribute, alias):
        # if this is not a foreign key, return alias and column name
        if not 'foreign-key' in attribute:
            column_name = attribute['name']
            # also return the source attribute, so we can check the type later
            return [f'{alias}.{column_name}']
        else:
            foreign_key = attribute['foreign-key']
            # but table names may need an alias
            attributes = foreign_key['attributes']
            target_name = foreign_key['table']
            target_alias = foreign_key.get('alias', target_name)
            # recurse into foreign key table to get the attributes
            return self._attributes(attributes, target_alias)
