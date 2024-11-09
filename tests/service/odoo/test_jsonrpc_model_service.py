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
