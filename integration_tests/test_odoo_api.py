import datetime

import pytest
import xmlrpc.client

# Odoo XML-RPC configuration
ODOO_URL = 'http://localhost:8069'
ODOO_DB = 'afas18'
ODOO_USERNAME = 'xxx'
ODOO_PASSWORD = 'xxx'

ODOO_URL = 'https://stml.odoo.com'
ODOO_DB = 'stml'  # Replace with your actual database name
ODOO_USERNAME = 'xxx'
ODOO_PASSWORD = 'xxx'


@pytest.fixture
def odoo_connection():
    # Initialize the XML-RPC clients
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')

    assert uid, "Failed to authenticate with Odoo XML-RPC"

    return {
        "models": models,
        "uid": uid
    }


def test_create_res_partner(odoo_connection):
    models = odoo_connection['models']
    uid = odoo_connection['uid']

    # Partner data
    partner_data = {
        'name': 'Test Partner',
        'email': 'test.partner@example.com',
        'phone': '1234567890'
    }

    # Create res.partner record
    partner_id = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'create', [partner_data]
    )

    # Check if partner_id is returned (indicating successful creation)
    assert partner_id, "Failed to create res.partner record"

    # Fetch the created partner record to verify its data
    created_partner = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'read', [[partner_id]]
    )[0]

    # Assertions to check if the created partner data matches the input data
    assert created_partner['name'] == partner_data['name']
    assert created_partner['email'] == partner_data['email']
    assert created_partner['phone'] == partner_data['phone']

    # Cleanup: Delete the created record after the test
    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'unlink', [[partner_id]])


def test_import_res_partner_data(odoo_connection):
    models = odoo_connection['models']
    uid = odoo_connection['uid']

    # Sample data to import multiple res.partner records
    data_to_import = [
        {'name': 'Imported Partner 1', 'email': 'imported1@example.com', 'phone': '1111111111'},
        {'name': 'Imported Partner 2', 'email': 'imported2@example.com', 'phone': '2222222222'},
    ]

    # Convert data to the format expected by Odoo's import API
    fields = ['name', 'email', 'phone']
    records = [[partner['name'], partner['email'], partner['phone']] for partner in data_to_import]

    # Import data to res.partner
    import_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'load', [fields, records]
    )['ids']

    # Check if all records were successfully created
    assert import_ids and len(import_ids) == len(data_to_import), "Failed to import res.partner records"

    # Fetch the imported records to verify the data
    imported_partners = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'read', [import_ids, fields]
    )

    # Assertions to check if the imported data matches the input data
    for i, partner in enumerate(data_to_import):
        assert imported_partners[i]['name'] == partner['name']
        assert imported_partners[i]['email'] == partner['email']
        assert imported_partners[i]['phone'] == partner['phone']

    # Cleanup: Delete the imported records after the test
    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'unlink', [import_ids])


def test_export_res_partner_data(odoo_connection):
    models = odoo_connection['models']
    uid = odoo_connection['uid']

    # Sample data to create for export test
    partners_to_create = [
        {'name': 'Export Partner 1', 'email': 'export1@example.com', 'phone': '3333333333'},
        {'name': 'Export Partner 2', 'email': 'export2@example.com', 'phone': '4444444444'},
    ]

    # Create sample records
    partner_ids = []
    for partner_data in partners_to_create:
        partner_id = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            'res.partner', 'create', [partner_data]
        )
        partner_ids.append(partner_id)

    # Export data: use search_read to get details of the created records
    fields_to_export = ['name', 'email', 'phone']
    exported_partners = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'search_read',
        [[['id', 'in', partner_ids]], fields_to_export]
    )

    # Assertions to check if the exported data matches the created data
    for i, partner in enumerate(partners_to_create):
        assert exported_partners[i]['name'] == partner['name']
        assert exported_partners[i]['email'] == partner['email']
        assert exported_partners[i]['phone'] == partner['phone']

    # Cleanup: Delete the created records after the test
    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'unlink', [partner_ids])


def test_export_res_partner_with_company(odoo_connection):
    models = odoo_connection['models']
    uid = odoo_connection['uid']

    # Create a company to associate with the partners
    company_name = 'Test Company6'
    company_data = {'name': company_name}
    company_id = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.company', 'create', [company_data]
    )

    # Sample data to import partners with associated company
    partners_to_import = [
        ['Export Partner 1', 'export1@example.com', '3333333333', company_name],
        ['Export Partner 2', 'export2@example.com', '4444444444', company_name],
    ]

    # Fields to import
    fields = ['name', 'email', 'phone', 'company_id']

    # Import partner data using the 'load' method
    import_result = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'load', [fields, partners_to_import]
    )

    # Verify import was successful and get partner IDs
    partner_ids = import_result.get('ids', [])
    assert partner_ids and len(partner_ids) == len(partners_to_import), "Failed to import res.partner records"

    # Export data: use search_read to get details of the imported records, including company information
    fields_to_export = ['name', 'email', 'phone', 'company_id']
    exported_partners = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'search_read',
        [[['id', 'in', partner_ids]], fields_to_export]
    )

    # Verify that the exported data matches the imported data, including company details
    for i, partner_data in enumerate(partners_to_import):
        assert exported_partners[i]['name'] == partner_data[0]
        assert exported_partners[i]['email'] == partner_data[1]
        assert exported_partners[i]['phone'] == partner_data[2]
        assert exported_partners[i]['company_id'][0] == company_id  # Check company_id
        assert exported_partners[i]['company_id'][1] == company_data['name']  # Check company name

    # Cleanup: Delete the imported partner and company records after the test
    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'unlink', [partner_ids])
    # models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.company', 'unlink', [[company_id]])

def test_export_res_partner_with_company(odoo_connection):
    models = odoo_connection['models']
    uid = odoo_connection['uid']

    # Create a company name with human readable time suffix to make it unique
    company_name = 'Test Company ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Create a company to associate with the partners
    company_data = {'name': company_name}
    company_id = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.company', 'create', [company_data]
    )

    # Sample data to create partners with associated company (only company_id as integer)
    partners_to_create = [
        {'name': 'Export Partner 1', 'email': 'export1@example.com', 'phone': '3333333333', 'company_id': company_id},
        {'name': 'Export Partner 2', 'email': 'export2@example.com', 'phone': '4444444444', 'company_id': company_id},
    ]

    # Create partner data using the 'create' method
    partner_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'create', [partners_to_create]
    )

    # Verify that the partners were created successfully
    assert partner_ids and len(partner_ids) == len(partners_to_create), "Failed to import res.partner records"

    # Export data: use search_read to get details of the imported records, including company information
    fields_to_export = ['name', 'email', 'phone', 'company_id']
    exported_partners = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        'res.partner', 'search_read',
        [[['id', 'in', partner_ids]], fields_to_export]
    )

    # Verify that the exported data matches the created data, including company details
    for i, partner_data in enumerate(partners_to_create):
        assert exported_partners[i]['name'] == partner_data['name']
        assert exported_partners[i]['email'] == partner_data['email']
        assert exported_partners[i]['phone'] == partner_data['phone']
        assert exported_partners[i]['company_id'][0] == company_id  # Check company_id directly

    # Cleanup: Delete the imported partner and company records after the test
    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'unlink', [partner_ids])
    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.company', 'unlink', [[company_id]])
