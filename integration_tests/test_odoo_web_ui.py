import csv
import os

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

# Odoo credentials
ODOO_URL = 'http://localhost:8069'
USERNAME = 'xxx'
PASSWORD = 'xxx'

@pytest.fixture(scope="module")
def setup_browser():
    # Initialize the WebDriver (ensure the correct path to your WebDriver)
    driver = webdriver.Chrome()  # or webdriver.Firefox(), depending on your browser
    yield driver
    driver.quit()

def test_export_contacts(setup_browser):
    driver = setup_browser
    driver.get(ODOO_URL)
    time.sleep(2)

    # Log in to Odoo
    driver.find_element(By.NAME, "login").send_keys(USERNAME)
    driver.find_element(By.NAME, "password").send_keys(PASSWORD + Keys.RETURN)

    # Wait for the dashboard to load
    time.sleep(2)

    # goto to contacts
    driver.get(ODOO_URL + "/odoo/contacts?view_type=list")

    time.sleep(2)

    # Select the first contact to export (you can select more as needed)
    first_contact_checkbox = driver.find_element(By.XPATH, "//table[@class='o_list_view']/tbody/tr[1]/td[1]/div/input")
    first_contact_checkbox.click()

    # Click on the "Action" dropdown
    action_dropdown = driver.find_element(By.XPATH, "//button[contains(text(), 'Action')]")
    action_dropdown.click()

    # Click on the "Export" option
    export_option = driver.find_element(By.XPATH, "//div[contains(text(), 'Export')]")
    export_option.click()

    # Wait for the export dialog to appear
    time.sleep(2)

    # Choose to export all fields or specific fields
    export_all_radio = driver.find_element(By.XPATH, "//input[@name='export_format' and @value='all']")
    export_all_radio.click()

    # Click the "Export" button
    export_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Export')]")
    export_button.click()

    # Wait for the export to complete (this may vary depending on your setup)
    time.sleep(5)

    # Optionally, you can add verification logic here to check for a download link or exported file

    # Log out (optional)
    # Click on the user menu
    user_menu = driver.find_element(By.XPATH, "//div[@class='oe_topbar_name']")
    user_menu.click()

    # Click on the logout option
    logout_option = driver.find_element(By.XPATH, "//a[contains(text(), 'Log out')]")
    logout_option.click()

    # Wait before ending the test
    time.sleep(2)


def create_csv_file(filename, contacts):
    """Create a CSV file with contact data."""
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'email', 'phone'])  # Header row
        writer.writerows(contacts)


def test_import_contacts(setup_browser):
    driver = setup_browser
    driver.get(ODOO_URL)
    time.sleep(2)

    # Log in to Odoo
    driver.find_element(By.NAME, "login").send_keys(USERNAME)
    driver.find_element(By.NAME, "password").send_keys(PASSWORD + Keys.RETURN)
    time.sleep(2)

    # Navigate to Contacts (can't navigate to import directly)
    driver.get(ODOO_URL + "/odoo/contacts")
    time.sleep(2)

    # Navigate to Contacts import )
    driver.get(ODOO_URL + "/odoo/contacts/import")
    time.sleep(2)

    # Create CSV data
    contacts_to_import = [
        ['Import Partner 1', 'import1@example.com', '5555555555'],
        ['Import Partner 2', 'import2@example.com', '6666666666'],
    ]

    # Create a temporary CSV file for importing
    csv_filename = 'contacts_to_import.csv'
    create_csv_file(csv_filename, contacts_to_import)

    # Upload the CSV file
    file_input = driver.find_element(By.XPATH, "//input[@type='file']")
    file_input.send_keys(os.path.abspath(csv_filename))  # Use absolute path

    # Wait for file upload
    time.sleep(2)

    # Click on the "Import" button in the dialog
    import_confirm_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Import')]")
    import_confirm_button.click()

    # Wait for the import process to complete
    time.sleep(5)

    # Optionally, you can add verification logic here to check if the contacts were imported successfully

    # Clean up: Remove the temporary CSV file
    if os.path.exists(csv_filename):
        os.remove(csv_filename)

    # Log out (optional)
    user_menu = driver.find_element(By.XPATH, "//div[@class='oe_topbar_name']")
    user_menu.click()

    logout_option = driver.find_element(By.XPATH, "//a[contains(text(), 'Log out')]")
    logout_option.click()

    # Wait before ending the test
    time.sleep(2)