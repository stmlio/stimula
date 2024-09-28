from datetime import datetime
from itertools import chain

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

from stimula.service.context import cnx_context
from stimula.service.query_executor import OperationType


class Reporter:
    def create_post_report(self, tables, contents, contexts, execution_results, execute, commit, skiprows):

        summary = {
            'execute': execute,
            'commit': commit,
            'timestamp': datetime.now().isoformat()
        }

        # set username if available
        if cnx_context and hasattr(cnx_context, 'username'):
            summary['username'] = cnx_context.username

        try:
            # count number of rows in all contents.
            summary['rows'] = sum([self._count_rows(content, skiprows) for content in contents])
        except:
            # if counting fails, do nothing
            pass

        summary['total'] = {
            'operations': len(execution_results),
            'success': len([er for er in execution_results if er.success]),
            'failed': len([er for er in execution_results if not er.success]),
            'insert': len([er for er in execution_results if er.operation_type == OperationType.INSERT]),
            'update': len([er for er in execution_results if er.operation_type == OperationType.UPDATE]),
            'delete': len([er for er in execution_results if er.operation_type == OperationType.DELETE]),
        }

        # summarize successful operations
        summary['success'] = {'insert': len([er for er in execution_results if er.operation_type == OperationType.INSERT and er.success]),
                              'update': len([er for er in execution_results if er.operation_type == OperationType.UPDATE and er.success]),
                              'delete': len([er for er in execution_results if er.operation_type == OperationType.DELETE and er.success])}

        # summarize failed operations
        summary['failed'] = {'insert': len([er for er in execution_results if er.operation_type == OperationType.INSERT and not er.success]),
                             'update': len([er for er in execution_results if er.operation_type == OperationType.UPDATE and not er.success]),
                             'delete': len([er for er in execution_results if er.operation_type == OperationType.DELETE and not er.success])}

        # files
        files = [self._summarize_file(table, context, content) for table, context, content in zip(tables, contexts, contents)]

        # list execution results
        rows = list(chain(*[er.report(execute) for er in execution_results]))

        return {'summary': summary, 'files': files, 'rows': rows}

    def _summarize_file(self, table, context, content):
        return {'table': table, 'context': context, 'size': len(content), 'md5': self.md5_string(content)}

    def md5_string(self, input_string):
        # Create an MD5 hash object
        md5_hash = hashes.Hash(hashes.MD5(), backend=default_backend())

        # encode the input string, or leave if it's already byte like
        if isinstance(input_string, str):
            input_string = input_string.encode('utf-8')

            # Update the hash with the byte-encoded string
        md5_hash.update(input_string)

        # Finalize the hash and return the hexadecimal representation
        return md5_hash.finalize().hex()

        # Update the hash object with the byte-encoded string
        md5_hash.update(encoded_string)

        # Return the hexadecimal representation of the hash
        return md5_hash.hexdigest()

    def _count_rows(self, content, skiprows):
        # decode the input string, or leave if it's already a string
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        return len(content.strip().split('\n')) - skiprows
