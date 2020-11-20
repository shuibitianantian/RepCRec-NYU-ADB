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
        TODO: Change site status to down
        :return: None
        """
        self.up = False
        self.data_manager.clear_uncommitted_changes()

    def recover(self):
        """
        TODO: Change site status to up
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
        return self.snapshots[tick][var_id - 1]