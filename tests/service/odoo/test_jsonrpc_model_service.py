from stimula.stml.model import Entity, Attribute, Reference


def test_login(jsonrpc_client):
    jsonrpc_client.login()
    assert jsonrpc_client.uid is not None


def test_get_table(jsonrpc_model_service):
    model_name = 'res.partner'
    table = jsonrpc_model_service.get_table(model_name)
    assert table
    assert table.name == model_name


def test_columns(jsonrpc_model_service):
    model_name = 'res.partner'
    table = jsonrpc_model_service.get_table(model_name)
    columns = table.columns
    assert len(columns) > 0


def test_get_primary_keys(jsonrpc_model_service):
    model_name = 'res.partner'
    table = jsonrpc_model_service.get_table(model_name)
    primary_keys = jsonrpc_model_service.find_primary_keys(table)
    assert len(primary_keys) == 1
    assert primary_keys[0] == 'id'


def test_resolve_foreign_key_table(jsonrpc_model_service):
    model_name = 'res.partner'
    table = jsonrpc_model_service.get_table(model_name)
    column = table.columns['company_id']
    fk_table, fk_column = jsonrpc_model_service.resolve_foreign_key_table(table, column.name)
    assert fk_table.name == 'res.company'
    assert fk_column == 'id'


def test_search(jsonrpc_client):
    model_name = 'res.partner'
    ids = jsonrpc_client.execute_kw(model_name, 'search', [[]])
    assert ids
    record = jsonrpc_client.execute_kw(model_name, 'read', [ids[0]], {'fields': ['name']})
    assert record
    assert record[0]['name']


def test_search_read(jsonrpc_client):
    model_name = 'res.partner'
    records = jsonrpc_client.execute_kw(model_name, 'search_read', [[]], {'fields': ['name']})
    assert records
    assert records[0]['name']


def test_search_read_foreign_field(jsonrpc_model_service):
    model_name = 'res.partner'
    mapping = Entity(name=model_name, attributes=[
        Attribute(name='name', type='text'),
        Reference(name='company_id', table='res.company', target_name='id', attributes=[
            Attribute(name='name', type='text'),
        ])
    ])
    records = jsonrpc_model_service.read_table(mapping)
    # verify we have at least one company name
    company_names = [r['company_id(name)'] for r in records if r.get('company_id(name)')]
    assert company_names

def test_search_read_nested_foreign_fields(jsonrpc_model_service):
    model_name = 'res.partner'
    # header = 'company_id(name:currency_id(name:symbol):parent_path)'
    mapping = Entity(name=model_name, attributes=[
        Attribute(name='name', type='text'),
        Reference(name='company_id', table='res.company', target_name='id', enabled=True, attributes=[
            Attribute(name='name', type='text'),
            Reference(name='currency_id', table='res.currency', target_name='id', attributes=[
                Attribute(name='name', type='text'),
                Attribute(name='symbol', type='text'),
            ]),
            Attribute(name='parent_path', type='text'),
        ])
    ])

    records = jsonrpc_model_service.read_table(mapping)
    # verify we have at least one company name
    key = 'company_id(name:currency_id(name:symbol):parent_path)'
    currencies = [r[key] for r in records if r.get(key)]
    assert currencies