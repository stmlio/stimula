"""
This class renders a mapping into an update query.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from .foreign_where_renderer import ForeignWhereClauseRenderer
from .insert_renderer import FromClauseRenderer
from .where_renderer import WhereClauseRenderer
from ..model import Entity, AbstractAttribute, Attribute, Reference


class UpdateRenderer:
    """
        update c
        set c2 = c0.c0, c3 = b.b0, c4 = b0.b0
        from c as c0, b, b as b0
        left join a on b0.b2 = a.a0
        where c.c1 = 'c4' and c0.c1 = 'c2' and b.b1 = 'b1' and a.a1 = 'a2';
    """

    def render(self, mapping: Entity):
        update_clause = UpdateClauseRenderer().render(mapping)
        from_clause = FromClauseRenderer(False).render(mapping)
        where_clause = WhereClauseRenderer().render(mapping)
        foreign_where_clause = ForeignWhereClauseRenderer(False, False).render(mapping)

        result = f'{update_clause}{from_clause}{where_clause}'
        if foreign_where_clause:
            result += ' and ' + foreign_where_clause
        return result


class UpdateClauseRenderer:
    def render(self, mapping: Entity):

        # can't update unique columns
        clauses = [self._attribute(a) for a in mapping.attributes if not a.unique]

        # comma separate cells
        return f'update {mapping.name} set ' + ', '.join(clauses)

    def _attribute(self, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            # if key is set, then update json field
            if attribute.key:
                return f"{attribute.name} = jsonb_set({attribute.name}, '{{{attribute.key}}}', to_jsonb(:{attribute.parameter}::text))"

            return f'{attribute.name} = :{attribute.parameter}'

        if isinstance(attribute, Reference):
            # table names may need an alias
            target_alias = attribute.alias or attribute.table

            # no need to recurse
            return f'{attribute.name} = {target_alias}.{attribute.target_name}'
