from model.managers.DataManager import DataManager
from model.managers.LockManager import LockManager


class Site(object):
    def __init__(self, site_id):
        self.data_manager = DataManager(site_id)
        self.lock_manager = LockManager()
        self.up = True

    def fail(self):
        self.up = False

    def recover(self):
        self.up = True