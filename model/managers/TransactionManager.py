from algorithms.DeadLockDetector import *


class TransactionManager(object):
    """
    The transaction manager distributes operation and hold the information of the entire simulation, I would like to say
    this is more like a central control of the whole simulation

    :param self.transactions: A set to store all running transactions
    :param self.wait_for_graph: A Wait-For object to detect deadlock
    :param self.blocked: A list contains all blocked operations
    :param self.blocked_transactions: A set of blocked transactions
    :param self.sites: A list of all sites in the simulation
    """

    def __init__(self):
        self.transactions = {}
        self.wait_for_graph = WaitFor(self)

        # store all blocked transaction and its operations
        self.blocked = []

        # store all blocked transaction id
        self.blocked_transactions = set()

        # store Site object to these
        self.sites = []

    def retry(self, tick):
        """
        retry blocked operations (update blocked operations and blocked transactions)

        :return: None
        """
        op_b = []
        tx_b = set()
        for op in self.blocked:
            if not op.execute(tick, self, True):
                op_b.append(op)

                if op.get_op_t() == "end" and op.get_parameters()[0] not in tx_b:
                    continue

                tx_b.add(op.get_parameters()[0])

        self.blocked = op_b
        self.blocked_transactions = tx_b

    def _distribute_operation(self, operation, tick):
        succeed = operation.execute(tick, self)
        if not succeed:
            self.blocked.append(operation)

    def step(self, operation, tick):
        """
        Process the new operation, if this cause a deadlock, the youngest transaction will be aborted

        :param operation: new oepration
        :param tick: time
        :return: None
        """
        # 2 Steps:
        #   First, retry blocked transactions ans distribute it if possible
        #   Second, distribute the new operation
        self.retry(tick)
        self._distribute_operation(operation, tick)

        if operation.get_op_t() in {"R", "W"} and self.wait_for_graph.check_deadlock():
            t = self.get_youngest_transaction(self.wait_for_graph.get_trace())[0]
            self.abort(t, 2)

    def attach_sites(self, sites):
        """
        Attach sites to the transaction manager

        :param sites: A list of all sites
        :return: None
        """
        self.sites = sites

    def get_site(self, idx):
        """
        Get the site of given id

        :param idx: site id
        :return: Site
        """
        return self.sites[idx - 1]

    def get_youngest_transaction(self, trace):
        """
        Find the youngest transaction

        :param trace: The deadlock cycle
        :return: The youngest transaction
        """
        ages = [(t, self.transactions[t].tick) for t in trace]
        return sorted(ages, key=lambda x: -x[1])[0]

    # Abort given transaction
    # Steps:
    #   1. release locks
    #   2. revert transaction changes
    #   3. delete transaction in TM
    def abort(self, transaction_id, abort_type):
        """
        Abort the transaction

        :param transaction_id: The transaction to be aborted
        :param abort_type: Why does the transaction be aborted, 1 => site fail, 2 => dead lock, 3 => read-only no available version
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
        elif abort_type == 3:
            print(f"Transaction {transaction_id} aborted (read-only, no version available of the variable to read)")
        else:
            raise ValueError(f"Unknown abort type: {abort_type}")

