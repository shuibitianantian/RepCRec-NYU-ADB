from configurations import distinct_variable_counts, number_of_sites


class DataManager(object):
    @staticmethod
    def _init_db(idx):
        """
        Initialize data based on the site id
        :param idx: site id
        :return: list of value of variable
        """
        data = [None] * distinct_variable_counts

        for i in range(1, distinct_variable_counts + 1):
            if i % 2 == 0 or i % number_of_sites + 1 == idx:
                data[i - 1] = 10 * i
        return data

    def __init__(self, site_id):
        self.site_id = site_id
        self.data = self._init_db(site_id)

        # Initialize accessible flag, False for None variable, True for others
        self.is_accessible = [False if v is None else True for v in self.data]

        # Any change before commit will be stored in self.log
        # self.log is a key-value pair, each pair contains the transaction id and its change
        self.log = {}

    def clear_uncommitted_changes(self):
        self.log = {}

    def commit(self, transaction_id):
        """
        Commit changes and clear log.
        Note: Currently this function is not used, the commit logic is coded in End operation
        :return: None
        """
        self.data.update(self.log[transaction_id])
        self.log[transaction_id] = {}

    # Change accessible flag after recover, which means the non replicated variables can be write and read
    # any other variable can be write but can not be read before any write operation commit on it,
    # remember to change the flag when any write operation is committed in this site after recovery
    def disable_accessibility(self):
        """
        Change all read accessible flag of non replicated variable to False
        :return: None
        """
        for i in range(1, distinct_variable_counts + 1):
            if i % 2 != 0 and i % number_of_sites + 1 == self.site_id:
                self.is_accessible[i - 1] = True
            else:
                self.is_accessible[i - 1] = False

    def revert_transaction_changes(self, transaction_id):
        """
        Revert changes if transaction is going to be aborted
        :param transaction_id:
        :return: None
        """
        self.log.pop(transaction_id, None)

    def get_variable(self, idx):
        return self.data[idx - 1]

    def set_variable(self, idx, val):
        self.data[idx - 1] = val
    
    def check_accessibility(self, idx):
        """
        Check if the variable can be accessed in this site
        :param idx:
        :return:
        """
        return self.is_accessible[idx - 1]