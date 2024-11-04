import numpy

from stimula.service.query_executor import ExecutionResult, OperationType


def test_report():
    er = ExecutionResult(1, OperationType.INSERT, True, 1, 'table_name', 'query', {'param': 'value'}, 'context')
    report = er.report(True)

    expected = [{'line_number': 1, 'operation_type': OperationType.INSERT, 'success': True, 'rowcount': 1, 'table_name': 'table_name', 'query': 'query', 'params': {'param': 'value'}, 'context': 'context'}]

    assert report == expected

def test_report_with_nan_value():
    er = ExecutionResult(1, OperationType.INSERT, True, 1, 'table_name', 'query', {'param': numpy.nan}, 'context')
    report = er.report(True)

    expected = [{'line_number': 1, 'operation_type': OperationType.INSERT, 'success': True, 'rowcount': 1, 'table_name': 'table_name', 'query': 'query', 'params': {'param': ''}, 'context': 'context'}]

    assert report == expected
