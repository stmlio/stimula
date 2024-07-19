from stimula.header.header_lexer import HeaderLexer


def test_header():
    data = ' e(fg)[h=i:j=123]'
    lexer = HeaderLexer()
    tokens = list(lexer.tokenize(data))
    assert len(tokens) == 13


def test_header_with_quoted_specifier_value():
    data = 'abc[x="123":y="$=123"]'
    lexer = HeaderLexer()
    tokens = list(lexer.tokenize(data))
    assert len(tokens) == 10


def test_header_with_unquoted_placeholder():
    # no need to quote the filter field name placeholder $
    data = 'abc[x=$]'
    lexer = HeaderLexer()
    tokens = list(lexer.tokenize(data))
    assert len(tokens) == 6
