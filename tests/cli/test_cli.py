from stimula.cli.cli import StimulaCLI

result = {'summary': {'execute': True, 'commit': False, 'timestamp': '2024-09-27T23:17:37.738209', 'username': 'odoo', 'rows': 20, 'total': {'operations': 20, 'success': 19, 'failed': 1, 'insert': 20, 'update': 0, 'delete': 0}, 'success': {'insert': 19, 'update': 0, 'delete': 0}, 'failed': {'insert': 1, 'update': 0, 'delete': 0}}, 'files': [{'table': 'res_partner', 'context': 'Customers', 'size': 4769, 'md5': '6bd9957169ac30181d0dcaebb566ca96'}, {'table': 'res_partner', 'context': 'Customers', 'size': 4867, 'md5': 'dece663d180ca4a7d57d8840f47264eb'}], 'rows': [{'line_number': 0, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(email, phone, mobile, type, is_company, lang, name, complete_name, active) select :email, :phone, :mobile, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '10350', 'email': 'kurtwort@hotma.locim', 'phone': '31273.2.0909', 'mobile': '3109372.2.90', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Cynthai Wotnmar', 'complete_name': 'Cynthai Wotnmar', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 0, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, street2, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :street2, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '10350', 'street': '305 N Sepulvade Vlbd', 'street2': 'Stuie 5', 'city': 'Manhatten Beach', 'code': 'CA', 'code_1': 'US', 'zip': '90266', 'type': 'delivery', 'name_1': '305 N Sepulvade Vlbd', 'active': 'true', 'complete_name': '305 N Sepulvade Vlbd'}, 'context': 'Customers'}, {'line_number': 1, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '11332', 'phone': '4082065.0.63', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Aann Mia', 'complete_name': 'Aann Mia', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 1, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '11332', 'street': '7343 Racohn Veiw Tc', 'city': 'San Jose', 'code': 'CA', 'code_1': 'US', 'zip': '95132', 'type': 'delivery', 'name_1': '7343 Racohn Veiw Tc', 'active': 'true', 'complete_name': '7343 Racohn Veiw Tc'}, 'context': 'Customers'}, {'line_number': 2, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '15433', 'phone': '.028518.6724', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Ehamaync Salt', 'complete_name': 'Ehamaync Salt', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 2, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '15433', 'street': '12241 W SNTAA PAUAL TS', 'city': 'SANTA PAULA', 'code': 'CA', 'code_1': 'US', 'zip': '93060', 'type': 'delivery', 'name_1': '12241 W SNTAA PAUAL TS', 'active': 'true', 'complete_name': '12241 W SNTAA PAUAL TS'}, 'context': 'Customers'}, {'line_number': 3, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '11218', 'phone': '448.39497.80', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Kmi Alts', 'complete_name': 'Kmi Alts', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 3, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '11218', 'street': '911 CAINMO DEL LOS', 'city': 'VALLEJO', 'code': 'CA', 'code_1': 'US', 'zip': '94591', 'type': 'delivery', 'name_1': '911 CAINMO DEL LOS', 'active': 'true', 'complete_name': '911 CAINMO DEL LOS'}, 'context': 'Customers'}, {'line_number': 4, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '18498', 'phone': '921.675.7117', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Ayala Atsl', 'complete_name': 'Ayala Atsl', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 4, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, street2, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :street2, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '18498', 'street': '81 Ggorery Eanl', 'street2': 'Set 210', 'city': 'Pleasant Hill', 'code': 'CA', 'code_1': 'US', 'zip': '94523', 'type': 'delivery', 'name_1': '81 Ggorery Eanl', 'active': 'true', 'complete_name': '81 Ggorery Eanl'}, 'context': 'Customers'}, {'line_number': 5, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '65346', 'phone': '949.588.9159', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Krean Noojshn', 'complete_name': 'Krean Noojshn', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 5, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '65346', 'street': '42261 Laek Eofrst Rdive', 'city': 'Lake Forest', 'code': 'CA', 'code_1': 'US', 'zip': '92630', 'type': 'delivery', 'name_1': '42261 Laek Eofrst Rdive', 'active': 'true', 'complete_name': '42261 Laek Eofrst Rdive'}, 'context': 'Customers'}, {'line_number': 6, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '65347', 'phone': '.138425.7546', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'N/A N/A', 'complete_name': 'N/A N/A', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 6, 'operation_type': 'INSERT', 'success': False, 'rowcount': 0, 'table_name': 'res_partner', 'params': {}, 'context': 'Customers', 'error': "Parsing error at token Token(type=':', value=':', lineno=1, index=0, end=1)"}, {'line_number': 7, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '13838', 'phone': '6.826781', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Junaita Tseralle', 'complete_name': 'Junaita Tseralle', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 7, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '13838', 'street': '2516 AORREBGS RD', 'city': 'APTOS', 'code': 'CA', 'code_1': 'US', 'zip': '95003', 'type': 'delivery', 'name_1': '2516 AORREBGS RD', 'active': 'true', 'complete_name': '2516 AORREBGS RD'}, 'context': 'Customers'}, {'line_number': 8, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '65348', 'phone': '5.2.64063080', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Jluia Pestit', 'complete_name': 'Jluia Pestit', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 8, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '65348', 'street': '1003 Thomospn', 'city': 'Long Beach', 'code': 'CA', 'code_1': 'US', 'zip': '90805', 'type': 'delivery', 'name_1': '1003 Thomospn', 'active': 'true', 'complete_name': '1003 Thomospn'}, 'context': 'Customers'}, {'line_number': 9, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': 'insert into res_partner(phone, type, is_company, lang, name, complete_name, active) select :phone, :type, :is_company, :lang, :name_1, :complete_name, :active returning res_partner.id', 'params': {'name': '17041', 'phone': '8.13685.1709', 'type': 'contact', 'is_company': 'false', 'lang': 'en_US', 'name_1': 'Kaern Estrella', 'complete_name': 'Kaern Estrella', 'active': 'true'}, 'context': 'Customers'}, {'line_number': 9, 'operation_type': 'INSERT', 'success': True, 'rowcount': 1, 'table_name': 'res_partner', 'query': "insert into res_partner(parent_id, street, city, state_id, zip, type, name, active, complete_name) select res_partner_1.id, :street, :city, res_country_state.id, :zip, :type, :name_1, :active, :complete_name from res_partner as res_partner_1 left join ir_model_data on res_partner_1.id = ir_model_data.res_id and ir_model_data.model = 'res.partner' and ir_model_data.module = 'netsuite_customer', res_country_state left join res_country on res_country_state.country_id = res_country.id where ir_model_data.name = :name and res_country_state.code = :code and res_country.code = :code_1", 'params': {'name': '17041', 'street': '741 Namoci Pacifico', 'city': 'Aptos', 'code': 'CA', 'code_1': 'US', 'zip': '95003', 'type': 'delivery', 'name_1': '741 Namoci Pacifico', 'active': 'true', 'complete_name': '741 Namoci Pacifico'}, 'context': 'Customers'}]}
def test_report_summary():
    report = StimulaCLI()._report_summary(result)
    expected = '''Rows read:  20
Operations: 20
Success:    19
Failed:     1
Customers:6 - "Parsing error at token Token(type=':', value=':', lineno=1, index=0, end=1)" 
Specify --commit (-C) to commit transaction.
'''
    assert report == expected

def test_report_verbose():
    report = StimulaCLI()._report_verbose(result)
    expected = '''Rows read:  20
Operations: 20
Success     19 (insert: 19, update: 0, delete: 0)
Failed      1 (insert: 1, update: 0, delete: 0)
Errors:
Customers:6 - "Parsing error at token Token(type=':', value=':', lineno=1, index=0, end=1)" - Query: "N/A"
Specify --commit (-C) to commit transaction.
'''
    assert report == expected

def test_report_audit():
    StimulaCLI()._report_audit(result)
