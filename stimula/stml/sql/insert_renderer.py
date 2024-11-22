"""
This class generates insert statements based on a mapping.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from typing import List

from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference
from stimula.stml.sql.foreign_where_renderer import ForeignWhereClauseRenderer
from stimula.stml.sql.select_renderer import SelectRenderer


class InsertRenderer:
    """
        header: 'c1[unique=true], c2(c1), c3(b1), c4(b2(a1))'

        query:
        insert into c (c1, c2, c3, c4)
        select :1, c.c0, b.b0, b1.b0
        from c, b, b as b0
        join a on b0.b2=a.a0
        where c.c1 = :2 and b.b1 = :3 and a.a1=:4
        returning id;
    """

    def render(self, mapping):
        insert_clause = InsertClauseRenderer().render(mapping)
        select_clause = SelectClauseRenderer().render(mapping)
        from_clause = FromClauseRenderer(True).render(mapping)
        where_clause = ForeignWhereClauseRenderer(True, False).render(mapping)
        returning_clause = ReturningClauseRenderer().render(mapping)

        # this where clause is also used in update query. Here we need 'where'
        if where_clause:
            where_clause = ' where ' + where_clause

        return f'{insert_clause}{select_clause}{from_clause}{where_clause}{returning_clause}'


class InsertClauseRenderer:
    def render(self, mapping: Entity):
        # get attributes. Skip extensions on base table, because they are not columns. We'll insert them in a separate query
        attributes = [a.name for a in mapping.attributes if not (isinstance(a, Reference) and a.extension)]

        # assert there's at least one attribute to insert
        assert attributes, f'No attributes to insert in table {mapping.name}'

        # comma separate
        return f'insert into {mapping.name}({', '.join(attributes)})'


class SelectClauseRenderer:
    def render(self, mapping: Entity):
        # get attributes, skip extensions on base table, because they are not columns. We'll insert them in a separate query
        attributes = [self._attribute(a) for a in mapping.attributes if not (isinstance(a, Reference) and a.extension)]

        # comma separate cells
        return ' select ' + ', '.join(attributes)

    def _attribute(self, attribute: AbstractAttribute):

        if isinstance(attribute, Attribute):
            if attribute.key:
                # build json object
                return f"jsonb_build_object('{attribute.key}', :{attribute.parameter})"

            # get value from parameter
            return f':{attribute.parameter}'

        if isinstance(attribute, Reference):
            # table names may need an alias
            target_alias = attribute.alias or attribute.table
            # no need to recurse
            return f'{target_alias}.{attribute.target_name}'


'''
There's a subtle difference in the from clause between insert and update statements with respect to extensions:
insert query: we must not join with extension table, because we're inserting a new record and we don't have a primary key yet.
update query: we must join with extension table, because we're updating an existing record and we need to filter by the extension table.
'''


class FromClauseRenderer:

    def __init__(self, is_insert_query):
        # if this is an insert query, we must not join with extension tables
        self._is_insert_query = is_insert_query

    def render(self, mapping: Entity):
        # compile and get result as list
        clauses = self.compile_as_list(mapping)

        if not clauses:
            return ''
        return ' from ' + ', '.join(clauses)

    def compile_as_list(self, mapping: Entity)-> List[str]:
        # compile attributes and return as list, so we can also use it to create ORM foreign key queries
        columns = [self._attribute(a, mapping.name) for a in mapping.attributes if isinstance(a, Reference)]

        # filter out empty columns that don't need a from clause
        return [column for column in columns if column]


    def _attribute(self, attribute: Reference, source_alias):

        # if foreign key is an extension on the root table, then we must not join if it's an insert query
        if attribute.extension and self._is_insert_query:
            return None

        # add target table
        from_clause = attribute.table

        # add alias
        target_alias = attribute.alias or attribute.table
        if target_alias != attribute.table:
            from_clause += f' as {target_alias}'

        # recurse
        return from_clause + self._attributes(attribute.attributes, target_alias)

    def _attributes(self, attributes, source_alias):
        # assert len(attributes) == 1, f'Can only insert a single attribute per column, found: {attributes}'
        from_clauses = [self._nested_attribute(a, source_alias) for a in attributes if isinstance(a, Reference)]
        return ''.join([c for c in from_clauses if c])

    def _nested_attribute(self, attribute: Reference, source_alias):

        # assume we need a left join. This may require a modifier at some point.
        from_clause = f' left join {attribute.table}'

        # add alias
        target_alias = attribute.alias or attribute.table
        if target_alias != attribute.table:
            from_clause += f' as {target_alias}'

        # add on clause
        from_clause += f' on {source_alias}.{attribute.name} = {target_alias}.{attribute.target_name}'

        # if this is an Odoo style extension relation and it's not on the root table, then we need to filter by qualifier (module) and table (model)
        if attribute.extension:
            # assume for now that source alias is the table name. This is fine as long as we're not joining the same table multiple times
            table_name = SelectRenderer().get_model_name(source_alias)
            from_clause += f' and {target_alias}.model = \'{table_name}\' and {target_alias}.module = \'{attribute.qualifier}\''

        # recurse
        return from_clause + self._attributes(attribute.attributes, target_alias)


class ReturningClauseRenderer:
    # returns 'returning id' if this mapping contains an extension relation on the root table

    def render(self, mapping: Entity):
        # get list of returned columns
        clauses = [self._attribute(a) for a in mapping.attributes]

        # filter out all empty clauses
        non_empty_clauses = [c for c in clauses if c]

        if not any(non_empty_clauses):
            return ''

        # assert there's no more than one
        assert len(non_empty_clauses) == 1, f'Can only return a single extension id, found: {non_empty_clauses}'

        return ' returning ' + mapping.name + '.' + non_empty_clauses[0]

    def _attribute(self, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            # not a foreign key relationship, so not an extension
            return None

        if isinstance(attribute, Reference):
            if not attribute.extension:
                # not an extension foreign key
                return None
            # extension, so return the id to return from the query. Default to 'id' bec/ that's what Odoo uses
            return attribute.id or 'id'
