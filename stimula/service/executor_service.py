"""
This class is used to execute queries in a transactional way. It will execute the queries in rounds until no new successful queries are found.

Author: Romke Jonker
Email: romke@stml.io
"""

import logging

from stimula.service.context import cnx_context
from stimula.service.query_executor import OperationType

_logger = logging.getLogger(__name__)


class ExecutorService:
    def execute_sql(self, query_executors, execute, commit, tx_size=1000):
        if not execute:
            # fake execution, return result
            return [qe.fake_execute() for qe in query_executors]

        # get cursor from context
        cr = cnx_context.cr

        # execute queries, rerun until exhausted
        result = self._eat_sleep_repeat(query_executors, cr, commit, tx_size)

        # commit if requested
        if commit:

            # registry may not be available during unit tests
            if hasattr(cnx_context, 'registry'):
                # registry may not have clear_cache() method
                if hasattr(cnx_context.registry, 'clear_cache'):
                    # invalidate caches to avoid stale values coming from cache
                    cnx_context.registry.clear_cache()
                else:
                    _logger.warning("Registry has no clear_cache() method")

        return result

    def _eat_sleep_repeat(self, query_executors, cr, commit, tx_size):
        # execute in rounds until no new successful queries are found

        # create result lists
        completed = []
        failed = []

        # copy query executors list
        remaining = query_executors.copy()

        done = False
        tx_count = 0

        while not done:
            new_completed_executors = []
            new_completed_results = []
            # iterate query executors
            for query_executor in remaining:
                # create or replace savepoint
                self.create_savepoint()
                # delegate execution to query executor
                execution_result = query_executor.execute(cr)
                # if successful
                if execution_result.success:
                    # append result to list
                    new_completed_results.append(execution_result)
                    # remove from remaining
                    new_completed_executors.append(query_executor)
                    # increment tx count
                    tx_count += 1
                    if tx_count >= tx_size:
                        # commit transaction
                        if commit:
                            cnx_context.cnx.commit()
                        else:
                            # rollback all queries
                            cnx_context.cnx.rollback()
                        # reset tx count
                        tx_count = 0
                else:
                    # rollback to savepoint
                    self.rollback_to_savepoint()
                    # append to failed list
                    failed.append(execution_result)
            if new_completed_results:
                # append new completed to completed list
                completed.extend(new_completed_results)
                # remove completed executors from remaining
                remaining = [qe for qe in remaining if qe not in new_completed_executors]
                # reset failed list and start again
                failed = []
            else:
                # nothing new completed, we're done
                done = True
                # commit if transactions remain
                if tx_count > 0 and commit:
                    cnx_context.cnx.commit()

        # combine completed and failed lists
        all_results = completed + failed

        # set delete queries apart, because they don't have line numbers
        deleted = [result for result in all_results if result.operation_type == OperationType.DELETE]
        insert_and_updates = [result for result in all_results if result.operation_type != OperationType.DELETE]

        # sort by line_number
        insert_and_updates.sort(key=lambda x: x.line_number)

        # append deleted to the end
        return insert_and_updates + deleted

    def create_savepoint(self):
        # create savepoint
        cnx_context.cr.execute("SAVEPOINT stimula_savepoint")

    def rollback_to_savepoint(self):
        # rollback to savepoint
        cnx_context.cr.execute("ROLLBACK TO SAVEPOINT stimula_savepoint")
