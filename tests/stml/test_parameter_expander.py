from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.parameter_expander import ParameterExpander


def test_no_parameters():
    mapping = Entity('table', [])
    result = ParameterExpander().expand(mapping, {})
    expected = [mapping]
    assert result == expected

def test_none():
    mapping = Entity('table', [None])
    result = ParameterExpander().expand(mapping, {})
    expected = [mapping]
    assert result == expected


def test_get_placeholders():
    mapping = Entity('table', [
        Attribute('column', default_value='${value}')
    ])
    result = ParameterExpander()._get_placeholders(mapping)
    expected = ['value']
    assert result == expected


def test_missing_values():
    mapping = Entity('table', [
        Attribute('column', default_value='${value}')
    ])
    substitutions = {}
    try:
        ParameterExpander().expand(mapping, substitutions)
        assert False
    except ValueError as e:
        assert str(e) == 'No value found for placeholders: [\'value\']'


def test_default_value():
    mapping = Entity('table', [
        Attribute('column', default_value='${value}')
    ])
    substitutions = {'value': {'default': ''}}
    result = ParameterExpander().expand(mapping, substitutions)
    expected = [Entity('table', [
        Attribute('column', default_value='default')
    ])]
    assert result == expected


def test_default_value_in_reference():
    mapping = Entity('table', [
        Reference('column', default_value='${value}')
    ])
    substitutions = {'value': {'default': ''}}
    result = ParameterExpander().expand(mapping, substitutions)
    expected = [Entity('table', [
        Reference('column', default_value='default')
    ])]
    assert result == expected


def test_default_exp_key():
    mapping = Entity('table', [
        Attribute('column', default_value='${def_value}', exp='${exp_value}', key='${key_value}')
    ])
    substitutions = {'def_value': {'default1': '', 'default2': ''}, 'exp_value': {'exp1': '', 'exp2': ''}, 'key_value': {'key1': ''}}
    result = ParameterExpander().expand(mapping, substitutions)
    expected = [
        Entity('table', [Attribute('column', default_value='default1', exp='exp1', key='key1')]),
        Entity('table', [Attribute('column', default_value='default1', exp='exp2', key='key1')]),
        Entity('table', [Attribute('column', default_value='default2', exp='exp1', key='key1')]),
        Entity('table', [Attribute('column', default_value='default2', exp='exp2', key='key1')])
    ]
    assert result == expected
