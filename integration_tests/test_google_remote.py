from integration_tests.test_local import call_main

REMOTE = 'http://localhost:8069/'
DB_NAME = 'afas18'
# REMOTE = 'https://stmlio-stimula-odoo-17-0-15543090.dev.odoo.com/'
# DB_NAME = 'stmlio-stimula-odoo-17-0-15543090'
DB_USER = 'admin'
DB_PASS = 'admin'
SHEET_ID = '1NwrH7ltvAh0zi_RtdcQr4hMH6Jcmu96XcK9BYr0k9Xk'


def _test_google_auth():
    call_main(f'stimula google -G ../google_client_secret.json -V')


def test_auth():
    call_main(f'stimula auth -r {REMOTE} -d {DB_NAME} -u {DB_USER} -p {DB_PASS} -V')


def test_post_single_file():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f res_partner -IU -V')


def test_post_multiple_files():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f res_users res_partner -IU -V -a')


def test_post_stml_file():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f company_user.stml -IU -V')


def test_post_multiple_stml_files():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f Delivery1.stml Contact.stml -IUC -V -a')
