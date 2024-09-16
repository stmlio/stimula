from integration_tests.test_local import call_main

REMOTE = 'http://localhost:8069/'
DB_NAME = 'beauty8'
# REMOTE = 'https://stmlio-stimula-odoo1-dev-14859116.dev.odoo.com/'
# DB_NAME = 'stmlio-stimula-odoo1-dev-14859116'
DB_USER = 'admin'
DB_PASS = 'admin'
SHEET_ID = '1xmESHdfahhCoEfc5rESEdIl6v-UHFBP9W5u5WB2pUE4'


def _test_google_auth():
    call_main(f'stimula google -G ../google_client_secret.json -V')


def test_auth():
    call_main(f'stimula auth -r {REMOTE} -d {DB_NAME} -u {DB_USER} -p {DB_PASS} -V')


def test_post_single_file():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f res_partner -e IUE -V')


def test_post_multiple_files():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f res_users res_partner -e IUE -V')


def test_post_stml_file():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f customer_delivery.stml -e IUEC -V')


def test_post_multiple_stml_files():
    call_main(f'stimula post -r {REMOTE} -g {SHEET_ID} -f customer_contact.stml customer_delivery.stml -e IUE -V')
