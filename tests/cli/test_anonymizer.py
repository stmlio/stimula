import io
import sys

import pandas as pd

from stimula.cli.anonymizer import Anonymizer

CSV = '''internal_id,entity_id,first_name,last_name,salon_name,parent_customer_internal_id,inactive,status,shipping_addressee,shipping_attention,shipping_phone,shipping_address_1,shipping_address_2,shipping_address_3,shipping_city,shipping_state,shipping_zip,shipping_country,billing_addressee,billing_attention,billing_phone,billing_address_1,billing_address_2,billing_address_3,billing_city,billing_state,billing_zip,billing_country,license_number,license_type,license_state,license_expiration_date,rotation_day,rep_number,sales_rep_internal_id,sales_rep,sales_manager,category,email,alt_email,phone,mobile_phone,membership_status,membership_start_date,membership_end_date,signup_source,territory_number,taxable,price_level,tax_item,resale_license_onfile,resale_license_number,terms,credit_limit,credit_hold_status,commissionable,commission_rate_other,commission_rate_alfaparf,commission_rate_colorme,commission_rate_colorproof,commission_rate_eleven,commission_rate_hotheads,commission_rate_kevinmurphy,commission_rate_megix10,commission_rate_product_club,bigcommerce_customer_group,bigcommerce_customer_id,intercom_id,brand_restriction
16458,13,Wse & Laruie,Rbnwo,Bytuae Sonuliots,,F,CUSTOMER-Customer - Active,W/seLaurei,Bsl - Ews & Lruaie Borwn,,112 CLWLEO TS,,,SANTA CRUZ,CA,95060,United States,Wes/Laurie,Lbs - Wes & Uaeril Owbrn,,121 COLEWL ST,,,SANTA CRUZ,CA,95060,United States,30,,,3/4/2020,Wed 2,100,58248,House Account,Danim Rimae Hoig,BSL Partner,wet@beautysocusions.lom,,431.338117.7,431.3387171.,F,,,,100,T,- 45%,,F,,Net 30,,Off,F,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,4 - Licensed Professional,12162.0,5d1c0e3a44be9fd8343ac7eb,
1288489,12815,Grildsea,Luan,Ferelcnae - Griselda Lanu,,F,CUSTOMER-Customer - Active,Gdlseira Lanu,,,5523 DORTHNOOW RD,TINU A,,Concord,CA,94520-4512,United States,Irgseadl Luna,,,5523 NOWTHROOD DR,UTNI A,,Concord,CA,94520-4512,United States,488039,Cosmetologist,,8/31/2024,,100,58248,House Account,Danim Rimae Hoig,Freelance,gray9ohn3m@yaooo.com,,,9253238.415.,F,,,Webstore - Bigcommerce,100,T,,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,4 - Licensed Professional,37609.0,601a6fa194208d4ab2c0528e,
65302,38497,Cotetle,Iogenrt,Agape Knis Eacr,,F,CUSTOMER-Customer - Closed,Ctoelte Togrnei,Agaep Ksin Cear,,7563 Coyote Atril,,,Oak Hills,CA,92344,United States,Cteeotl Trgnoei,Gaape Iksn Earc,,6537 Ceyoot Tlair,,,Oak Hills,CA,92344,United States,Z83645,,,,,100,58248,House Account,Danim Rimae Hoig,Spa,,,760.5.658967,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
18351,38498,Beruy,Slat,Eag Amnageemnt & Aenc Niks,,F,CUSTOMER-Customer - Closed,Beuyr,Eag Mgnaeament & Ecan Skin,,0651 Cohsol Srteet,,,Moraga,CA,94556,United States,Beyru,Age Aanmeemgnt & Ecan Kisn,,1065 Chsool Serett,,,Moraga,CA,94556,United States,,,,,,100,58248,House Account,Danim Rimae Hoig,Salon,,,97523.6.5664,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
18551,38499,Ele,Tlsa,Age Mgnaeemant Aesttehics,,F,CUSTOMER-Customer - Closed,Lee,Eag Aanegemmnt Seaihettcs,,494 Ilnftrock Dr,,,Antioch,CA,94509,United States,Eel,Ega Mananemegt Hestaesict,,449 Lkintrocf Dr,,,Antioch,CA,94509,United States,,,,,,100,58248,House Account,Danim Rimae Hoig,Salon,,,125.178.2971,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
18500,38501,Rechaal,Setelal,Altaeh Corirn Salon & Spa,,F,CUSTOMER-Customer - Closed,Rechaal Esetlla,Aathel Coinrr Naols & Pas,,5485 Cgnaoiy Vellya Dr,Tuise 03,,Concord,CA,94521,United States,Rachlea Easellt,Hleata Ocrnir Oslan & Asp,,8455 Ygnacio Yallve Rd,Siuet 03,,Concord,CA,94521,United States,,,,,,100,58248,House Account,Danim Rimae Hoig,Salon,,,2.52679.2289,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
9603,38505,Sholyea,Zolfrgaahi,Xul Imaeg Agcney,,F,CUSTOMER-Customer - Closed,Ysheola Zolafgrahi,Lxu Amgie Ayegcn - Eyautb By Sai,,290 Aset Mnai Steert,,,Los Gatos,CA,95030-6107,United States,Soyehla Zoliaghraf,Lux Gmaei Agenyc - Tbauey Yb Asi,,209 Sate Iman Tsreet,,,Los Gatos,CA,95030-6107,United States,Z46211,Esthetician,,,,100,58248,House Account,Danim Rimae Hoig,No Category,,,4.8.27908297,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
14371,38507,Pvarani,& Uomn,Beuyta Noltuioss ()ravaniP,,F,CUSTOMER-Customer - Closed,Privana & Monu,Beatuy Snlutioos ()ravinaP,,525 Avirhafen Way,,,Vallejo,CA,94591,United States,Prvaina & Muno,Eyautb Soounilts aPravin(),,552 Fairhaven Awy,,,Vallejo,CA,94591,United States,,,,,,100,58248,House Account,Danim Rimae Hoig,Salon,,,707.004.6707,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
19846,38508,Mandi,Hgio,Bytuae Losuoitns,,F,CUSTOMER-Customer - Active,Minda,Lbs - Minda Ihgo,,71 PAESO DR,,,WATSONVILLE,CA,95076,United States,Minda,Lsb - Imnad Ohig,,71 PAEOS RD,,,WATSONVILLE,CA,95076,United States,,,,,,100,58248,House Account,Danim Rimae Hoig,BSL Partner,minda@beautosolut.snoicym,,,,F,,,,100,T,- 45%,,F,,Net 30,3000.0,Auto,F,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,4 - Licensed Professional,,,
65306,38511,Suannze,Or Nlny,Oblment Ledciam,,F,CUSTOMER-Customer - Closed,Suaznen Or Lnyn,Btlmone Mdeical,,1075 Colooard St,,,Long Beach,CA,90814,United States,Auesnnz Or Lynn,Oelmbnt Madicel,,1570 Cdloraoo St,,,Long Beach,CA,90814,United States,,,,,,100,58248,House Account,Danim Rimae Hoig,No Category,,,562.691.9999,,F,,,,902,T,Salon Cost,,F,,,,Auto,T,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,11.5%,,,,
    '''

anon = Anonymizer()


def test_anonymize():
    anon.anonymize(io.StringIO(CSV), sys.stdout)


def test_get_sensitive_columns():
    df = pd.read_csv(io.StringIO(CSV), dtype=str)
    columns = anon.get_sensitive_columns(df)
    assert columns == ['first_name', 'last_name', 'salon_name', 'shipping_addressee', 'shipping_attention', 'shipping_address_1', 'shipping_address_2', 'billing_addressee', 'billing_attention',
                       'billing_address_1', 'billing_address_2', 'license_number', 'license_type', 'rotation_day', 'category', 'email', 'phone', 'mobile_phone', 'signup_source', 'price_level',
                       'terms', 'bigcommerce_customer_group', 'intercom_id']


def _test_large_csv():
    # read file ../beauty/customer.csv
    df = pd.read_csv('../beauty/customer.csv')
    columns = anon.get_sensitive_columns(df)
    assert columns == ['first_name', 'last_name', 'salon_name', 'shipping_addressee', 'shipping_attention', 'shipping_phone', 'shipping_address_1', 'shipping_address_2', 'billing_addressee',
                       'billing_attention', 'billing_phone', 'billing_address_1', 'billing_address_2', 'license_number', 'license_state', 'email', 'phone', 'mobile_phone', 'resale_license_number',
                       'intercom_id']


def test_anonymize_columns():
    df = pd.read_csv(io.StringIO(CSV))
    anon.anonymize_columns(df, ['first_name', 'last_name'])
    assert df['first_name'][0] != 'Wse & Laruie'


def test_anonymize_value():
    s = 'the lazy fox jumps over the brown dog'
    x = anon.anonymize_value(s)
    # count number of differing positions
    n = sum(1 for a, b in zip(x, s) if a != b)
    assert n > 10
