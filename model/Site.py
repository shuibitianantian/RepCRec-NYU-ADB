from model.managers.DataManager import DataManager
from model.managers.LockManager import LockManager
from configurations import *
from copy import deepcopy


class Site(object):
    def __init__(self, site_id):
        self.site_id = site_id
        self.data_manager = DataManager(site_id)
        self.lock_manager = LockManager()

        # Flag to indicate site status
        self.up = True
        # Snapshots for multi-version read consistency
        self.snapshots = {}

    def fail(self):
        """
        Change site status to false and clear all uncommitted changes in this site
        :return: None
        """
        self.up = False
        self.data_manager.clear_uncommitted_changes()
        self.lock_manager.clear()

    def echo(self):
        """
        return a list of variable values
        :return: all variable values to prettyTable (which will be printed in dump operation)
        """
        prefix = f"Site {self.site_id} ({'up' if self.up else 'down'})"
        return [prefix] + [v for v in self.data_manager.data]

    def recover(self):
        """
        Change site status to true, and disable all read accessibility of replicated variable
        :return: None
        """
        self.up = True
        self.data_manager.disable_accessibility()

    def snapshot(self, tick):
        """
        For multi-version consistency, take snapshot of current data
        :param tick: time
        :return: None
        """
        self.snapshots[tick] = deepcopy(self.data_manager.data)

    def get_snapshot_variable(self, tick, var_id):
        """
        query variable data from snapshot of given tick
        :param tick: time
        :param var_id: variable id
        :return: variable value
        """
        return self.snapshots[tick][var_id - 1]
