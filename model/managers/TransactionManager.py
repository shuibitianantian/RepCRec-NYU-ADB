from algorithms.DeadLockDetector import *


class TransactionManager(object):
    def __init__(self):
        self.transactions = {}
        self.wait_for_graph = WaitFor(self)

        # store all blocked transaction and its operations
        self.blocked = []

        # store Site object to these
        self.sites = []

    def retry(self, tick):
        """
        retry blocked operations
        :return: None
        """
        self.blocked = [op for op in self.blocked if not op.execute(tick, self)]

    def _distribute_operation(self, operation, tick):
        succeed = operation.execute(tick, self)
        if not succeed:
            self.blocked.append(operation)

    def step(self, operation, tick):
        # 2 Steps:
        #   First, retry blocked transactions ans distribute it if possible
        #   Second, distribute the new operation
        if self.wait_for_graph.check_deadlock():
            t = self.get_youngest_transaction(self.wait_for_graph.get_trace())[0]
            self.abort(t, 2)

        self.retry(tick)
        self._distribute_operation(operation, tick)

    def attach_sites(self, sites):
        self.sites = sites

    def get_site(self, idx):
        return self.sites[idx - 1]

    def get_youngest_transaction(self, trace):
        ages = [(t, self.transactions[t].tick) for t in trace]
        return sorted(ages, key=lambda x: -x[1])[0]

    # Abort given transaction
    # Steps:
    #   1. release locks
    #   2. revert transaction changes
    #   3. delete transaction in TM
    def abort(self, transaction_id, abort_type):
        """
        :param transaction_id: The transaction to be aborted
        :param abort_type: Why does the transaction be aborted, 1 => site fail / 2 => dead lock
        :return: None
        """
        for site in self.sites:
            if site.up:
                site.lock_manager.release_transaction_locks(transaction_id)
                site.data_manager.revert_transaction_changes(transaction_id)

        # Remove any blocked operation belongs to this transaction
        self.blocked = [op for op in self.blocked if op.get_parameters()[0] != transaction_id]

        # Remove transaction in wait graph
        self.wait_for_graph.remove_transaction(transaction_id)

        self.transactions.pop(transaction_id)
        if abort_type == 1:
            print(f"Transaction {transaction_id} aborted (site failure)")
        elif abort_type == 2:
            print(f"Transaction {transaction_id} aborted (deadlock)")
        else:
            raise ValueError(f"Unknown abort type: {abort_type}")

