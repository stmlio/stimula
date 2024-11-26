from stimula.service.db import DB


def test_create_substitutions_map():
    csv = '''\
    domain, name, substition
    Color,	White, White
    Color,	Weiß, White 
    Color,	Blanc, White
    Size,	Small/Medium, S/M
    Size,	Large, L
    Size,10,10
    '''
    map = DB()._create_substitutions_map(csv)

    assert map['color']['weiß'] == 'White'
    assert map['size']['large'] == 'L'
