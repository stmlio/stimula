import pytest
import requests
import json

# Odoo credentials and URL
ODOO_URL = 'http://localhost:8069'
DB_NAME = 'afas18'  # Replace with your actual database name
USERNAME = 'xxx'
PASSWORD = 'xxx'

# ODOO_URL = 'https://stmlio-stimula-odoo1-afas-orm-16042618.dev.odoo.com'
# DB_NAME = 'stmlio-stimula-odoo1-afas-orm-16042618'  # Replace with your actual database name
# USERNAME = 'xxx'
# PASSWORD = 'xxx'

# ODOO_URL = 'https://stml.odoo.com'
# DB_NAME = 'stml'  # Replace with your actual database name
# USERNAME = 'xxx'
# PASSWORD = 'xxx'

@pytest.fixture(scope="module")
def session():
    """Fixture to create a requests session for JSON-RPC login."""
    with requests.Session() as s:
        # Log in to Odoo and store the session cookies
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "common",
                "method": "login",
                "args": [DB_NAME, USERNAME, PASSWORD],
            },
            "id": 1,
        }
        url = f"{ODOO_URL}/jsonrpc"
        response = s.post(url, json=payload)
        assert response.status_code == 200, "Failed to reach the Odoo JSON-RPC endpoint"

        response_data = response.json()
        if 'error' in response_data:
            assert False, f"Login failed: {response_data['error']}"

        yield s  # Yield the session for use in tests

def test_create_import_record(session):
    # Prepare the payload to create a new import record
    payload = {
        "id": 8,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute_kw",
            "args": [
                DB_NAME,
                2,
                PASSWORD,
                "base_import.import",
                "create",
                [{"res_model": "res.partner"}],
                {
                    "context": {
                        "lang": "en_US",
                        "tz": "Europe/Amsterdam",
                        "uid": 2,
                        "allowed_company_ids": [1]
                    }
                }
            ]
        }
    }

    # Perform the POST request to create the import record
    url = f"{ODOO_URL}/jsonrpc"
    response = session.post(url, json=payload)

    # Check if the request was successful
    assert response.status_code == 200, "Failed to create the import record"

    # Parse the response
    response_data = response.json()

    # Check for errors in the response
    if 'error' in response_data:
        assert False, f"Failed to create import record: {response_data['error']}"

    assert 'result' in response_data, "Response does not contain a result"

    # Optionally, print or log the result
    print(f"Import record created successfully: {response_data['result']}")

@pytest.fixture
def get_odoo_session_id():
    """Authenticate with Odoo and return session ID."""
    login_url = f"{ODOO_URL}/web/session/authenticate"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": DB_NAME,
            "login": USERNAME,
            "password": PASSWORD
        },
        "id": 1
    }
    response = requests.post(login_url, data=json.dumps(payload), headers=headers)
    result = response.json()
    assert response.status_code == 200, "Failed to connect to Odoo."
    assert "result" in result, "Authentication failed; check credentials."
    return result["result"]["session_id"]

def test_get_contacts(get_odoo_session_id):
    """Test to retrieve contacts from Odoo database."""
    session_id = get_odoo_session_id
    assert session_id, "Session ID not retrieved; authentication failed."

    # Define JSON-RPC request
    url = f"{ODOO_URL}/web/dataset/call_kw"
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.partner",  # Odoo model for contacts
            "method": "search_read",  # method to retrieve records
            "args": [
                [],  # empty list means no filters, retrieve all contacts
                ["id", "name", "email", "phone"]  # fields to retrieve
            ],
            "kwargs": {}
        },
        "id": 1
    }

    # Send request
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    assert response.status_code == 200, "Failed to retrieve contacts."

    contacts = response.json().get("result", [])
    assert contacts, "No contacts retrieved; check if there are contacts in the database."

    # Verify that each contact has required fields
    for contact in contacts:
        assert "id" in contact, "Contact missing 'id'."
        assert "name" in contact, "Contact missing 'name'."
        # Optionally check for optional fields like email and phone
        # assert "email" in contact, "Contact missing 'email'."
        # assert "phone" in contact, "Contact missing 'phone'."

    print("Test passed! Contacts retrieved successfully.")


def test_get_contacts(session):
    """Test to retrieve contacts from Odoo database using an authenticated session."""
    url = f"{ODOO_URL}/jsonrpc"
    headers = {"Content-Type": "application/json"}

    payload = {
        "id": 8,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute_kw",
            "args": [
                DB_NAME,
                2,
                PASSWORD,
                "res.partner",
                "search_read",
                [[], ["id", "name", "email", "phone"]],
                {
                    "context": {
                        "lang": "en_US",
                        "tz": "Europe/Amsterdam",
                        "uid": 2,
                        "allowed_company_ids": [1]
                    }
                }
            ]
        }
    }


    # Send request using the authenticated session
    response = session.post(url, data=json.dumps(payload), headers=headers)
    assert response.status_code == 200, "Failed to retrieve contacts."

    contacts = response.json().get("result", [])
    assert contacts, "No contacts retrieved; check if there are contacts in the database."

    # Verify that each contact has the required fields
    for contact in contacts:
        assert "id" in contact, "Contact missing 'id'."
        assert "name" in contact, "Contact missing 'name'."

    print("Test passed! Contacts retrieved successfully.")

def test_create_contact(session):
    """Test to retrieve contacts from Odoo database using an authenticated session."""
    url = f"{ODOO_URL}/jsonrpc"
    headers = {"Content-Type": "application/json"}

    payload = {
        "id": 8,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute_kw",
            "args": [
                DB_NAME,  # Database name
                2,  # User ID (make sure this is the correct user ID)
                PASSWORD,  # User password
                "res.partner",  # Model name for contacts
                "create",  # Method to create a new record
                [{
                    "name": "New Contact Name",  # Name of the contact
                    "email": "new_contact@example.com",  # Email of the contact
                    "phone": "+123456789",  # Phone number of the contact
                    "company_id": 1,  # Company ID (if applicable)
                    # Add any additional fields as necessary
                }],
                {
                    "context": {
                        "lang": "en_US",
                        "tz": "Europe/Amsterdam",
                        "uid": 2,  # User ID
                        "allowed_company_ids": [1]  # Allowed company IDs
                    }
                }
            ]
        }
    }

    # Send request using the authenticated session
    response = session.post(url, data=json.dumps(payload), headers=headers)
    assert response.status_code == 200, "Failed to retrieve contacts."

    contacts = response.json().get("result", [])
    assert contacts, "No contacts retrieved; check if there are contacts in the database."

    # Verify that each contact has the required fields
    for contact in contacts:
        assert "id" in contact, "Contact missing 'id'."
        assert "name" in contact, "Contact missing 'name'."

    print("Test passed! Contacts retrieved successfully.")

def create_payload_to_get_models():
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "service": "object",
            "method": "execute_kw",
            "args": [
                DB_NAME,
                2,  # User ID (make sure this is the correct user ID)
                PASSWORD,
                "ir.model",  # Model for metadata about models
                "search_read",  # Method to retrieve models
                [],
                {
                    "context": {
                        "lang": "en_US",
                        "tz": "Europe/Amsterdam",
                        "uid": 2,
                        "allowed_company_ids": [1]
                    }
                }
            ]
        }
    }
    return payload


def test_retrieve_models():
    payload = create_payload_to_get_models()

    response = requests.post(
        f"{ODOO_URL}/jsonrpc",
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )

    assert response.status_code == 200, response.get('text', "Failed.")

    result = response.json().get("result", [])

    models = [model["model"] for model in result]

    # print result with json formatting
    print(json.dumps(models, indent=2))
