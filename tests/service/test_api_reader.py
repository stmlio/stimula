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


def test_afas_fileconnector_api():
    guid = '7A1CEDB642077E2D8C7E97A7EBBB7234'
    name = '1106156.pdf'
    url = 'https://50677.rest.afas.online/profitrestservices/fileconnector/{guid}/{name}'
    auth = 'AfasToken PHRva2VuPjx2ZXJzaW9uPjE8L3ZlcnNpb24+PGRhdGE+M0I4QkE5N0RDNUVFNEM1MUIyRDdFRTUzQ0VDRUMyMEMwOTAzRjcwMkIwMTA0OEJFODhBQTcwM0JGM0VGQzk2ODwvZGF0YT48L3Rva2VuPg=='
    params = {'guid': guid, 'name': name}

    attribute = Attribute('attachment', url=url, auth=auth)
    document = ApiReader().read_document(attribute, params)

    assert document is not None

    # read document as pdf
    assert document[:4] == b'%PDF'
    assert len(document) > 10000

def test_afas_fileconnector_with_underscores():
    # check that we can download an attachment that failed during migration
    # guid = '503527AB485B2FBD915362AD5BCFD80D'
    guid = '2EA200344DBF88FC9304E4B84DA7ADD2'
    name = 'factuur_201301597.pdf'
    # name = 'Factuur_5F20130115631.pdf'
    # name = '2013000823.pdf'
    fixed_name = name.replace('_', '_5F')
    url = 'https://50677.rest.afas.online/profitrestservices/fileconnector/{guid}/{name}'
    auth = 'AfasToken PHRva2VuPjx2ZXJzaW9uPjE8L3ZlcnNpb24+PGRhdGE+M0I4QkE5N0RDNUVFNEM1MUIyRDdFRTUzQ0VDRUMyMEMwOTAzRjcwMkIwMTA0OEJFODhBQTcwM0JGM0VGQzk2ODwvZGF0YT48L3Rva2VuPg=='
    params = {'guid': guid, 'name': fixed_name}

    attribute = Attribute('attachment', url=url, auth=auth)
    document = ApiReader().read_document(attribute, params)

    assert document is not None

    # read document as pdf
    assert document[:4] == b'%PDF'
    assert len(document) > 10000
