"""
This class merges two mapping headers into a single mapping. The purpose is to let the user submit a mapping header and then
present him/her with a merged mapping header that contains the requested columns and the default columns in the correct order.

The requirements for the merged mapping are:
    - requested columns appear in order as requested
    - requested columns replace default columns, matched by attribute names
    - default columns appear in their own order, 'as much as possible'.
    - only requested columns are enabled


Author: Romke Jonker
Email: romke@rnadesign.net
"""
from stimula.stml.model import Entity, AbstractAttribute


class StmlMerger:
    def merge(self, default_mapping: Entity, requested_mapping: Entity) -> Entity:

        # verify both mappings have the same table
        assert default_mapping.name == requested_mapping.name

        # disable all default columns by removing 'in-use' attribute
        for a in default_mapping.attributes:
            a.enabled = False

        # map default columns by column key
        default_attributes_map = {a.name: a for a in default_mapping.attributes}

        # map requested columns by column key, but only for columns that have attributes
        requested_attributes_map = {a.name: a for a in requested_mapping.attributes if a}

        # merge requested columns into default columns
        result_attributes_map = self._merge_maps(default_attributes_map, requested_attributes_map, self._merge_attribute)

        # convert result column map to list
        result_attributes = list(result_attributes_map.values())

        return Entity(default_mapping.name, result_attributes)


    def _merge_attribute(self, default_attribute: AbstractAttribute, requested_attribute: AbstractAttribute) -> AbstractAttribute:
        # return requested column, but keep 'in-use' attribute from default column
        requested_attribute.in_use = default_attribute.in_use

        # also keep the 'primary-key' attribute from default column
        requested_attribute.primary_key = default_attribute.primary_key

        return requested_attribute

    def _merge_maps(self, a, b, f=lambda x, y: y):
        '''
        Sorts two dictionaries by their 'key' attribute.
        Resulting dictionary has:
        - all keys from a and b
        - values from b have precedence over values from a
        - order in b is preserved
        - order in a is preserved for the longest possible tail
        '''
        # get keys from b that do not exist in a

        # sort keys in a by their position in b
        b_keys = (list(b.keys()))
        i = len(b_keys) - 1
        c_keys = []
        for key in reversed(a.keys()):
            if key in c_keys:
                # already visited
                pass
            elif not key in b_keys:
                # not in b, simply add to c
                c_keys.insert(0, key)
            else:
                while i >= 0 and key != b_keys[i]:
                    # must still add, but keep items from b in order
                    c_keys.insert(0, b_keys[i])
                    i -= 1
                if i >= 0:
                    # found key in b, add to c
                    c_keys.insert(0, key)
                    i -= 1
        # get remaining keys from b
        remainders = b_keys[:i + 1]
        # insert remainders at the front of c_keys
        c_keys = remainders + c_keys

        # merge dictionaries, using lambda f to merge values that appear in both dictionaries
        merged = {**a, **b}
        for key in a.keys() & b.keys():
            merged[key] = f(a[key], b[key])

        # sort by c_keys
        result = {k: merged[k] for k in c_keys}

        return result
