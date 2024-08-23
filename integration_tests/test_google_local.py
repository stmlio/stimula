from integration_tests.test_local import call_main

DB_NAME = 'beauty'
DB_USER = 'odoo'
DB_PASS = 'odoo'
STIMULA_KEY = "secret"
SHEET_ID = '1xmESHdfahhCoEfc5rESEdIl6v-UHFBP9W5u5WB2pUE4'


def _test_auth():
    call_main(f'stimula google -G ../google_client_secret.json -V')


def test_post_single_file():
    call_main(f'stimula post -k {STIMULA_KEY} -g {SHEET_ID} -f res_partner -e IUE -V')


def test_post_multiple_files():
    call_main(f'stimula post -k {STIMULA_KEY} -g {SHEET_ID} -f res_users res_partner -e IUE -V')


def test_post_stml_file():
    call_main(f'stimula post -k {STIMULA_KEY} -g {SHEET_ID} -f customer.stml -e IUE -V')


def test_post_multiple_stml_files():
    call_main(f'stimula post -k {STIMULA_KEY} -g {SHEET_ID} -f customer_contact.stml customer_delivery.stml -e IUE -V')
