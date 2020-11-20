from algorithms.DeadLockDetector import *


class TransactionManager(object):
    def __init__(self):
        self.transactions = {}
        self.wait_for_graph = WaitFor()

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
        self.retry(tick)
        self._distribute_operation(operation, tick)

    def attach_sites(self, sites):
        self.sites = sites

    def get_site(self, idx):
        return self.sites[idx - 1]

