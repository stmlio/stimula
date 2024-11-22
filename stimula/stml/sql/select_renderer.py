"""
This class compiles a mapping into a select query.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import re
from itertools import chain
from typing import List, Tuple

from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class SelectRenderer:
    """
        select c.c1, c0.c1, b.b1, a.a1
        from c
        left join c as c0 on c.c2 = c0.c0
        left join b on c.c3 = b.b0
        left join b as b0 on c.c4 = b0.b0
        left join a on b0.b2=a.a0
        order by c.c1
    """

    def render(self, mapping: Entity, where_clause=None):
        """
        Compiles a mapping into a select query
        :param mapping: the mapping
        :param where_clause: a free where clause for the caller to specify
        :return: the select query
        """
        select_clause = SelectClauseRenderer().render(mapping)
        join_clause = JoinClauseRenderer().render(mapping)
        order_by_clause = OrderByClauseRenderer().render(mapping)

        # render [filter="...$..."] headers into a where clause, replacing '$' with the column name
        where = WhereClauseRenderer().render(mapping, where_clause)
        return f'{select_clause} from {mapping.name}{join_clause}{where}{order_by_clause}'

    def compile_count_query(self, mapping: Entity, where_clause=None):
        table = mapping.name
        aliased_mapping = AliasEnricher().enrich(mapping)
        select_clause = 'select count(*)'
        join_clause = JoinClauseRenderer().render(aliased_mapping)
        where = f' where {where_clause}' if where_clause else ''
        return f'{select_clause} from {table}{join_clause}{where}'

    def get_model_name(self, alias):
        # this method gets the model name to use in the extension clause
        # current solution is based on the alias, which is not very robust
        # alias is of the form 'xxx_xxx_xxx_d', we need to convert that into 'xxx.xxx.xxx'

        # remove '_0' postfix from alias using regex if it exists
        alias = re.sub(r'_\d+$', '', alias)
        return alias.replace('_', '.')


class SelectClauseRenderer:
    def render(self, mapping: Entity):

        # Include empty columns. Skip cells with skip=true or orm-only modifier. We need those when reading CSV, but not when reading from DB
        attributes = [self._attribute(a, mapping.name) for a in mapping.attributes if (not a) or (not a.skip and not a.orm_only)]

        # join attributes per column
        joined_columns = [self._join_attributes(a) for a in attributes]

        # comma separate columns
        return 'select ' + ', '.join(joined_columns)

    def _attribute(self, attribute: AbstractAttribute, alias) -> List[Tuple]:
        # column may be empty
        if not attribute:
            return [('\'\'',)]

        if isinstance(attribute, Attribute):
            # if there's a key, then return the json field
            if attribute.key:
                return [(f"{alias}.{attribute.name}->>'{attribute.key}'", attribute)]

            # return alias and column name. Also return the source attribute, so we can check the type later
            return [(f'{alias}.{attribute.name}', attribute)]

        if isinstance(attribute, Reference):
            # table names may need an alias
            target_alias = attribute.alias or attribute.table

            # recurse into foreign key table to get the attributes
            return self._nested_attributes(attribute.attributes, target_alias)

    def _nested_attributes(self, attributes: List[AbstractAttribute], alias: str) -> List[Tuple]:

        # flatten the attributes for this column
        return list(chain(*[self._attribute(p, alias) for p in attributes]))

    def _join_attributes(self, selects: List[Tuple]) -> 'str':
        # if there's more than one attribute, we must cast jsonb columns to string
        if len(selects) > 1:
            selects = self._cast_selects(selects)

        # get names
        names = [p[0] for p in selects]

        # colon separate attributes
        return ' || \':\' || '.join(names)

    def _cast_selects(self, selects):
        # postgres can't concatenate jsonb columns to anything, so cast to string
        cast_selects = []
        for select in selects:
            # select is a tuple of (column, attribute)
            if select[1].type == 'jsonb':
                cast_selects.append((f'{select[0]}::text', select[1]))
            else:
                cast_selects.append(select)
        return cast_selects


class JoinClauseRenderer:
    def render(self, mapping: Entity):
        table_name = mapping.name
        # glue cells together, skip empty columns
        return ''.join([self._attribute(a, table_name) for a in mapping.attributes if a])

    def _attribute(self, attribute: AbstractAttribute, alias):
        # if no foreign key, return empty string
        if isinstance(attribute, Attribute):
            return ''

        if isinstance(attribute, Reference):
            # but table names may need an alias
            source_name = attribute.name
            target_name = attribute.target_name
            target_table = attribute.table
            target_alias = attribute.alias or target_table

            # Assume we need a left join. This may require a modifier at some point.
            join_clause = f' left join {target_table}'

            # only add 'as alias' if it's different from table name
            if target_alias != target_table:
                join_clause += f' as {target_alias}'
            join_clause += f' on {alias}.{source_name} = {target_alias}.{target_name}'

            # if this is an Odoo style extension relation, then we need to filter by qualifier (module) and table (model)
            if attribute.qualifier:
                # for an extension table, we should do a double join instead of a left join, because we want to skip records that don't have an extension record
                join_clause = join_clause.replace(' left join ', ' join ')
                # assume for now that alias is the table name. This is fine as long as we're not joining the same table multiple times
                table_name = SelectRenderer().get_model_name(alias)
                join_clause += f' and {target_alias}.model = \'{table_name}\' and {target_alias}.module = \'{attribute.qualifier}\''

            # recurse
            return join_clause + self._attributes(attribute.attributes, target_alias)

    def _attributes(self, attributes: List[AbstractAttribute], alias):
        attribute_list = [self._attribute(attribute, alias) for attribute in attributes]
        return ''.join(attribute_list)


class OrderByClauseRenderer:
    def render(self, mapping: Entity):
        table_name = mapping.name
        # clauses to order by, skip empty columns
        clauses = [self._attribute(a, table_name) for a in mapping.attributes if a and a.unique]

        # nothing to order by
        if not clauses:
            return ''

        return ' order by ' + ', '.join(clauses)

    def _attribute(self, attribute: AbstractAttribute, alias: str):
        if isinstance(attribute, Attribute):
            # if no foreign key, get value from parameter
            return f'{alias}.{attribute.name}'

        if isinstance(attribute, Reference):
            # but table names may need an alias
            attributes = attribute.attributes
            target_name = attribute.table
            target_alias = attribute.alias or target_name
            # recurse
            return ', '.join([self._attribute(p, target_alias) for p in attribute.attributes])


class WhereClauseRenderer:
    def render(self, mapping: Entity, where_clause):
        """
        Compiles [select="...$..."] modifiers into a where clause
        :param mapping:
        :param where_clause: additional where clause to append
        :return: the where clause
        """
        table_name = mapping.name

        # get conditions from columns with filter modifiers, skip empty columns
        conditions = list(chain(*[self._attribute(a, table_name) for a in mapping.attributes if a]))

        # append user-defined where clause
        if where_clause:
            conditions.append(where_clause)

        if not conditions:
            return ''

        # return where clause with conditions joined by ' and '
        return ' where ' + ' and '.join(conditions)

    def _attribute(self, attribute, alias) -> List[str]:
        # if this is not a foreign key, return alias and column name
        if isinstance(attribute, Attribute):
            if not attribute.filter:
                return []

            # also return the source attribute, so we can check the type later
            name = f'{alias}.{attribute.name}'
            return [attribute.filter.replace('$', name)]

        if isinstance(attribute, Reference):
            # table names may need an alias
            target_alias = attribute.alias or attribute.target_name
            # recurse into foreign key table to get the attributes
            return self._attributes(attribute.attributes, target_alias)

    def _attributes(self, attributes: List[AbstractAttribute], alias):
        # flatten list
        return list(chain(*[self._attribute(p, alias) for p in attributes]))
