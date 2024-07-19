from .sly import Lexer


class HeaderLexer(Lexer):
    # Set of token names.   This is always required
    tokens = {ID, VALUE, QUOTED_VALUE, COLON, EQUALS, LPAREN, RPAREN, LBRACK, RBRACK}

    # String containing ignored characters between tokens
    ignore = ' \t'

    # Regular expression rules for tokens
    ID = r'[a-zA-Z_][a-zA-Z0-9_-]*'
    VALUE = r'[a-zA-Z0-9_$]+'
    QUOTED_VALUE = r'"[^\"]+"'
    COLON = r'\:'
    EQUALS = r'\='
    LPAREN = r'\('
    RPAREN = r'\)'
    LBRACK = r'\['
    RBRACK = r'\]'
