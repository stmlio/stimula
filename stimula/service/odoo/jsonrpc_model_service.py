"""
ModelService implementation for Odoo JSON-RPC
"""
import json

import requests

from stimula.service.model_service import ModelService


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
        assert 'result' in response, response.get('error', "Request failed.")
        return response.get('result')


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
