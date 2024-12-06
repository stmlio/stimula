from stimula.service.api_reader import ApiReader
from stimula.stml.model import Attribute


def test_read_document():
    # test that ApiReader can read a document from a URL
    url = 'https://api.stml.io/fileconnector/{guid}/{name}'
    params = {'guid': '12345', 'name': 'attachment.pdf'}
    attribute = Attribute('attachment', url=url)
    document = ApiReader().read_document(attribute, params)
    #     assert that the document is not empty
    assert document is not None
    assert len(document) > 1000


def _test_afas_fileconnector_api():
    guid = '7A1CEDB642077E2D8C7E97A7EBBB7234'
    name = '1106156.pdf'
    url = 'https://50677.rest.afas.online/profitrestservices/fileconnector/{guid}/{name}'
    auth = 'AfasToken xxx'
    params = {'guid': guid, 'name': name}

    attribute = Attribute('attachment', url=url, auth=auth)
    document = ApiReader().read_document(attribute, params)

    assert document is not None

    # read document as pdf
    assert document[:4] == b'%PDF'
