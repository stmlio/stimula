from itertools import chain

from stimula.service.query_executor import OperationType


class Reporter:
    def create_post_report(self, tables, contents, execution_results, execute, commit):
        insert = len([er for er in execution_results if er.operation_type == OperationType.INSERT])
        update = len([er for er in execution_results if er.operation_type == OperationType.UPDATE])
        delete = len([er for er in execution_results if er.operation_type == OperationType.DELETE])

        found = {'insert': insert, 'update': update, 'delete': delete}

        summary = {'found': found, 'execute': execute, 'commit': commit}

        result = {'summary': summary}

        # only set success & failed if execute is True
        if execute:
            # summarize successful operations
            summary['success'] = {'insert': len([er for er in execution_results if er.operation_type == OperationType.INSERT and er.success]),
                                  'update': len([er for er in execution_results if er.operation_type == OperationType.UPDATE and er.success]),
                                  'delete': len([er for er in execution_results if er.operation_type == OperationType.DELETE and er.success])}

            # summarize failed operations
            summary['failed'] = {'insert': len([er for er in execution_results if er.operation_type == OperationType.INSERT and not er.success]),
                                 'update': len([er for er in execution_results if er.operation_type == OperationType.UPDATE and not er.success]),
                                 'delete': len([er for er in execution_results if er.operation_type == OperationType.DELETE and not er.success])}

        # list execution results
        rows = list(chain(*[er.report(execute) for er in execution_results]))
        result['rows'] = rows

        return result
