from stimula.service.api_reader import ApiReader


def test_read_document():
    # test that ApiReader can read a document from a URL
    url = 'https://api.stml.io/fileconnector/{guid}/{name}'
    params = {'guid': '12345', 'name': 'attachment.pdf'}
    document = ApiReader().read_document(url, params)
    #     assert that the document is not empty
    assert document is not None
    assert len(document) > 1000
