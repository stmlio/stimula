"""
This class generates delete statements based on a mapping.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from typing import List

from stimula.stml.model import Entity, AbstractAttribute, Reference
from stimula.stml.sql.foreign_where_renderer import ForeignWhereClauseRenderer
from stimula.stml.sql.insert_renderer import ReturningClauseRenderer
from stimula.stml.sql.where_renderer import WhereClauseRenderer


class DeleteRenderer:
    """
        delete from c
        using c
        left join b on c.c0 = b.b0
        where c.c1 = :c1 and b.b1 = :b1;
    """

    def render(self, mapping: Entity):
        using_clause = UsingClauseRenderer().render(mapping)
        where_clause = WhereClauseRenderer().render(mapping)
        foreign_where_clause = ForeignWhereClauseRenderer(False, True).render(mapping)
        # in a delete statement, the foreign where clause always needs an 'and'
        foreign_where_clause = ' and ' + foreign_where_clause if foreign_where_clause else ''
        # returning clause is needed if we need to delete from an extension table
        returning_clause = ReturningClauseRenderer().render(mapping)

        result = f'delete from {mapping.name}{using_clause}{where_clause}{foreign_where_clause}{returning_clause}'

        return result


class UsingClauseRenderer:
    # this class is similar to FromClauseCompiler, but it only uses unique columns
    def render(self, mapping: Entity):
        # get clauses for unique foreign keys
        clauses = [self._attribute(a) for a in mapping.attributes if a.unique and isinstance(a, Reference)]

        if not clauses:
            return ''
        return f' using ' + ', '.join(clauses)

    def _attribute(self, attribute: Reference) -> str:

        # add target table
        from_clause = attribute.table

        # add alias
        target_alias = attribute.alias or attribute.table
        if target_alias != attribute.table:
            from_clause += f' as {target_alias}'

        # recurse
        return from_clause + ''.join(self._nested_attributes(attribute.attributes, target_alias))

    def _nested_attributes(self, attributes: List[AbstractAttribute], source_alias) -> List[str]:
        # get attributes that contains a foreign key
        return [self._nested_attribute(a, source_alias) for a in attributes if isinstance(a, Reference)]

    def _nested_attribute(self, attribute: Reference, source_alias) -> str:

        # if not root, then assume we need a left join. This may require a modifier at some point.
        from_clause = ' left join '

        # add target table
        from_clause += attribute.table

        # add alias
        target_alias = attribute.alias or attribute.table
        if target_alias != attribute.table:
            from_clause += f' as {target_alias}'

        from_clause += f' on {source_alias}.{attribute.name} = {target_alias}.{attribute.target_name}'

        # recurse
        return from_clause + ''.join(self._nested_attributes(attribute.attributes, target_alias))
