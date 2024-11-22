"""
ModelService implementation for Odoo JSON-RPC
"""
import json
from itertools import chain
from typing import List

import requests

from stimula.service.model_service import ModelService
from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class JsonRpcModelService(ModelService):
    def __init__(self, client):
        self.client = client

    def get_table(self, name):
        tables = self.client.execute_kw('ir.model', 'search_read', [[['model', '=', name]]], {'fields': ['id', 'model', 'field_id']})
        if not tables:
            raise ValueError(f"Table '{name}' not found")
        if len(tables) > 1:
            raise ValueError(f"More than one table found with name '{name}'")
        table = tables[0]
        return Table(self.client, table['model'], table['field_id'])

    def find_primary_keys(self, table):
        columns = table.columns
        # in Odoo, if id exists, then it's the primary key.
        # TODO: implement for many-to-many relationships
        return [columns['id'].name] if 'id' in columns else []

    def resolve_foreign_key_table(self, table, column_name):
        # get referred column
        if column_name not in table.columns:
            raise ValueError(f"Column '{column_name}' not found in table '{table.name}'")
        column = table.columns[column_name]
        assert column.relation, f"Column '{column_name}' does not have a relation"
        foreign_table = self.get_table(column.relation)
        # for Odoo, assume the foreign column is 'id'
        foreign_column_name = 'id'
        return foreign_table, foreign_column_name

    def get_non_empty_columns(self, table):
        return []

    def read_table(self, mapping: Entity, where_clause=None):
        # get model name
        model = mapping.name

        # read attribute values from the model
        records = self._read_columns(model, mapping.attributes)

        # convert each row from list of tuples to dictionary, skip unresolved foreign keys
        return [{f[1]: self._join_values(f[2]) for f in r if len(f) == 3} for r in records]

    def _read_columns(self, model, attributes: List[AbstractAttribute]):

        # get attribute names
        fields = [a.name for a in attributes]

        # perform search_read on the model
        records = self.client.execute_kw(model, 'search_read', [[]], {'fields': fields})

        # resolve foreign keys for each attribute
        records_resolved = list(zip(*[self._resolve_foreign_key(records, a) for a in attributes]))

        # return resolved records as list
        return records_resolved

    def _resolve_foreign_key(self, records, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            return [(r['id'], attribute.name, [r[attribute.name]]) for r in records]

        if isinstance(attribute, Reference):
            # get foreign key attribute name
            fk = attribute.name

            # list all unique foreign key ids
            ids = list(set([r[fk][0] for r in records if r.get(fk)]))
            if not ids:
                return

            # read all foreign key values, recurse to resolve nested foreign keys. Join keys and values for nested foreign keys.
            fk_records = self._read_attributes(attribute.table, attribute.attributes, ids)

            # create a dictionary of resolved foreign key records
            fk_records_dict = {r[0]: r for r in fk_records}

            # return foreign key values
            records_resolved = [self.create_fk_record(fk, fk_records_dict, r) for r in records]

            return records_resolved

    def create_fk_record(self, fk, fk_records_dict, record):
        # get foreign key value
        fk_value = record.get(fk)

        # if foreign key is empty or false, return empty record
        if not fk_value:
            return (record['id'],)

        # return foreign key record
        return (record['id'], f'{fk}({fk_records_dict.get(record[fk][0])[1]})', fk_records_dict.get(record[fk][0])[2])

    def _read_attributes(self, model, attributes: List[AbstractAttribute], ids):
        # get attribute names
        fields = [a.name for a in attributes]

        # perform search_read on the model
        records = self.client.execute_kw(model, 'search_read', [[['id', 'in', ids]]], {'fields': fields})

        # resolve foreign keys for each attribute
        records_resolved = list(zip(*[self._resolve_foreign_key(records, a) for a in attributes]))

        # join keys, but don't join values yet
        joined_records = [(r[0][0], ':'.join([f[1] for f in r]), list(chain(*[f[2] for f in r]))) for r in records_resolved]

        return joined_records

    def _join_values(self, values):
        if len(values) == 0:
            return ''
        if len(values) == 1:
            return values[0]
        return ':'.join([self._quote(v) for v in values])

    def _quote(self, value):
        # no need to quote other than string
        if not isinstance(value, str):
            return value

        # no need to quote value without a colon
        if not ':' in value:
            return value

        # else quote
        return f'"{value}"'


class JsonRpcClient:
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self._uid = None
        self._id = 0

    @property
    def id(self):
        self._id = self._id + 1
        return self._id

    @property
    def uid(self):
        if self._uid is None:
            self._uid = self.login()
        return self._uid

    def login(self):
        response = self.call('common', 'login', [self.db, self.username, self.password])
        assert response.get('result'), "Authentication failed; check credentials."
        return response["result"]

    def call(self, service, method, args):
        """Authenticate with Odoo and return session ID."""
        headers = {'Content-Type': 'application/json'}
        payload = {
            "id": self.id,
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": service,
                "method": method,
                "args": args,
            }
        }

        response = requests.post(self.url, data=json.dumps(payload), headers=headers)
        assert response.status_code == 200, response.get('text', "Failed.")

        return response.json()

    def execute_kw(self, model, method, args, kwargs={}):
        args = [
            self.db,
            self.uid,
            self.password,
            model,  # Model for metadata about models
            method,  # Method to retrieve models
            args,
            kwargs
        ]
        response = self.call('object', 'execute_kw', args)
        assert 'result' in response, response.get('error', "Request failed.").get('data', "Request failed.")
        return response.get('result')


class Table:
    def __init__(self, client, name, field_id):
        self.client: JsonRpcClient = client
        self.name: str = name
        self._field_id: list = field_id
        self._columns: list = None

    _type_map = {'char': 'text', 'integer': 'numeric', 'many2one': 'integer'}

    @property
    def columns(self):
        if self._columns is None:
            # get field names and types
            columns = self.client.execute_kw('ir.model.fields', 'search_read', [[['id', 'in', self._field_id]]], {'fields': ['name', 'ttype', 'relation']})
            # columns = self.client.execute_kw('ir.model.fields', 'search_read', [[['id', 'in', self._field_id]]], {})
            self._columns = {column['name']: Column(column['name'], self._substitute_type(column['ttype']), column['relation']) for column in columns}
        return self._columns

    def _substitute_type(self, type):
        # replace Odoo types with SQL types
        return self._type_map.get(type, type)

    def __str__(self):
        return self.name


class Column:
    def __init__(self, name, type, relation):
        self.name: str = name
        self.type: str = type
        self.relation: str = relation

    @property
    def quoted_name(self):
        return self.name

    def __str__(self):
        return self.name
