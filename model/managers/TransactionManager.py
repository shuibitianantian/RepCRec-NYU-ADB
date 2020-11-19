from algorithms.DeadLockDetector import *


class TransactionManager(object):
    def __init__(self):
        self.transactions = {}
        self.wait_for_graph = WaitFor()

        # store all blocked transaction and its operations
        self.blocked = set()

        # store Site object to these
        self.sites = []

    def retry(self):
        pass

    def _distribute_operation(self, operation, tick):
        operation.execute(tick, self)

    def step(self, operation, tick):
        # 2 Steps:
        #   First, retry blocked transactions ans distribute it if possible
        #   Second, distribute the new operation
        self.retry()
        self._distribute_operation(operation, tick)

    def attach_sites(self, sites):
        self.sites = sites

