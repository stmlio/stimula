from abc import ABC
from typing import List, Dict


class Entity:
    def __init__(self, name: str, attributes: List['AbstractAttribute'] = [], primary_key: str = None) -> None:
        self.name: str = name
        self.primary_key: str = primary_key
        self.attributes: List[AbstractAttribute] = attributes

    def to_dict(self) -> Dict[str, any]:
        data = {
            "name": self.name,
            "primary_key": self.primary_key,
            "attributes": [prop.to_dict() if prop else None for prop in self.attributes]
        }
        return {key: value for key, value in data.items() if value}

    def __repr__(self) -> str:
        return str(self.to_dict())  # Use to_dict for the repr method

    def __eq__(self, other: object) -> bool:
        # Check if other is an instance of Entity and compare relevant attributes
        if not isinstance(other, Entity):
            return False
        return self.name == other.name \
            and _none_safe_compare(self.primary_key, other.primary_key) \
            and self.attributes == other.attributes


class AbstractAttribute(ABC):
    def __init__(self, name: str, unique: bool, skip: bool, exp: str, default_value: str, orm_only: bool, enabled: bool, primary_key: bool, in_use: bool, default: bool, deduplicate: bool,
                 substitute: str) -> None:
        self.name: str = name
        self.unique: bool = unique
        self.skip: bool = skip
        self.exp: str = exp
        self.default_value: str = default_value
        self.orm_only: bool = orm_only
        self.enabled: bool = enabled
        self.primary_key: bool = primary_key
        self.in_use: bool = in_use
        self.default: bool = default
        self.deduplicate: bool = deduplicate
        self.substitute: str = substitute

    def to_dict(self) -> Dict[str, any]:
        data = {"name": self.name, 'unique': self.unique, 'skip': self.skip, 'exp': self.exp, 'default_value': self.default_value, 'orm_only': self.orm_only, 'enabled': self.enabled,
                'primary_key': self.primary_key, 'in_use': self.in_use, 'default': self.default, 'deduplicate': self.deduplicate, 'substitute': self.substitute}
        return {key: value for key, value in data.items() if value}

    def __repr__(self) -> str:
        return str(self.to_dict())  # Use to_dict for the repr method

    def __eq__(self, other: object) -> bool:
        # Check if other is an instance of Attribute and compare the name
        if not isinstance(other, AbstractAttribute):
            return False
        return self.name == other.name \
            and self.unique == other.unique \
            and self.skip == other.skip \
            and self.exp == other.exp \
            and self.default_value == other.default_value \
            and self.orm_only == other.orm_only \
            and self.enabled == other.enabled \
            and self.primary_key == other.primary_key \
            and self.in_use == other.in_use \
            and self.default == other.default \
            and self.deduplicate == other.deduplicate \
            and self.substitute == other.substitute


class Attribute(AbstractAttribute):
    def __init__(self, name: str, unique=False, skip=False, exp='', default_value=None, orm_only=False, enabled: bool = False, primary_key: bool = False, in_use: bool = False, default: bool = False,
                 deduplicate: bool = False, substitute: str = None,
                 key: str = None, type: str = None, parameter: str = None, filter: str = None, api: str = None, url: str = None) -> None:
        super().__init__(name, unique, skip, exp, default_value, orm_only, enabled, primary_key, in_use, default, deduplicate, substitute)
        self.key: str = key
        self.type: str = type
        self.parameter: str = parameter
        self.filter: str = filter
        self.api: str = api
        self.url: str = url

    def to_dict(self) -> Dict[str, any]:
        data = super().to_dict() | {"key": self.key, "type": self.type, "parameter": self.parameter, "filter": self.filter, "api": self.api, "url": self.url}
        return {key: value for key, value in data.items() if value}

    def __eq__(self, other: object) -> bool:
        # Check if other is an instance of Attribute and compare the name
        if not isinstance(other, Attribute):
            return False
        return super().__eq__(other) \
            and _none_safe_compare(self.key, other.key) \
            and _none_safe_compare(self.type, other.type) \
            and _none_safe_compare(self.parameter, other.parameter) \
            and _none_safe_compare(self.filter, other.filter) \
            and _none_safe_compare(self.api, other.api) \
            and _none_safe_compare(self.url, other.url)


class Reference(AbstractAttribute):
    def __init__(self, name: str, attributes: List['AbstractAttribute'] = [], unique: bool = False, skip: bool = False, exp: str = '', default_value: str = None, orm_only: bool = False,
                 enabled: bool = False, primary_key: bool = False, in_use: bool = False, default: bool = False, deduplicate: bool = False, substitute: str = None,
                 table: str = None, target_name: str = None, qualifier: str = None, extension: bool = False, alias: str = None, id: str = None) -> None:
        super().__init__(name, unique, skip, exp, default_value, orm_only, enabled, primary_key, in_use, default, deduplicate, substitute)
        self.attributes: List[AbstractAttribute] = attributes
        self.table = table
        self.target_name = target_name
        self.qualifier = qualifier
        self.extension = extension
        self.alias = alias
        self.id = id

    def to_dict(self) -> Dict[str, any]:
        data = super().to_dict() | {"table": self.table, "target_name": self.target_name, "qualifier": self.qualifier, "extension": self.extension, "alias": self.alias, "id": self.id}
        data['attributes'] = [a.to_dict() for a in self.attributes]
        return {key: value for key, value in data.items() if value}

    def __eq__(self, other: object) -> bool:
        # Check if other is an instance of Attribute and compare the name
        if not isinstance(other, Reference):
            return False
        return super().__eq__(other) \
            and self.attributes == other.attributes \
            and _none_safe_compare(self.table, other.table) \
            and _none_safe_compare(self.target_name, other.target_name) \
            and _none_safe_compare(self.qualifier, other.qualifier) \
            and self.extension == other.extension \
            and _none_safe_compare(self.alias, other.alias) \
            and _none_safe_compare(self.id, other.id)


def _none_safe_compare(a, b) -> bool:
    if a is None:
        return b is None
    return a == b
