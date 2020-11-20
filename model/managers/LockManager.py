
class Lock(object):
    def __init__(self, transaction_id, lock_type=0):
        self.type = lock_type
        self.transaction_id = transaction_id

    def promote(self, ):
        if self.type == 0:
            self.type = 1


class LockManager(object):
    # 0 represents share lock, 1 represent exclusive lock
    # variable: {0: set(transaction_id), 1: transaction_id}
    def __init__(self):
        self.lock_table = {}

    def try_lock_variable(self, transaction_id, variable_id, lock_type):
        """
        TODO: Testing needed, cannot guarantee no bug in current code
        :param transaction_id:
        :param variable_id:
        :param lock_type:
        :return: if lock variable succeed or not
        """
        # Make sure given lock type is 0 or 1
        if lock_type != 0 and lock_type != 1:
            raise ValueError(f"Unknown lock type: {lock_type}")

        # Case 1: no lock on give variable
        elif variable_id not in self.lock_table:
            # Situation 1: shared lock, initialize shared lock on the variable
            # and add transaction_id to the shared lock list
            if lock_type == 0:
                self.lock_table[variable_id] = {0: {transaction_id}, 1: None}

            # Situation 2: exclusive lock, initialize exclusive lock on the variable
            # and change the exclusive flag to give transaction_id
            elif lock_type == 1:
                self.lock_table[variable_id] = {0: set(), 1: transaction_id}
            return True

        # Case 2: some locks on given variable
        else:
            # Situation 1: shared lock, no exclusive lock exists, add the transaction_id in the shared lock list
            if lock_type == 0 and self.lock_table[variable_id][1] is None:
                self.lock_table[variable_id][0].add(transaction_id)
                return True

            # Situation 2: shared lock, exclusive lock exists, reject lock the variable for the transaction
            elif lock_type == 0 and self.lock_table[variable_id][1] is not None:
                return False

            # Situation 3: Exclusive lock, any existing lock will conflict with it
            elif lock_type == 1:
                # Situation 3.1: given transaction has a shared lock on given variable id (promote lock)
                if transaction_id in self.lock_table[variable_id][0] and len(self.lock_table[variable_id][0]) == 1:
                    self.lock_table[variable_id][0].remove(transaction_id)
                    self.lock_table[variable_id][1] = transaction_id
                    return True
                else:
                    return False

    def try_unlock_variable(self, variable_id, transaction_id):
        """
        TODO: Testing needed, cannot guarantee no bug in current code
        :param variable_id:
        :param transaction_id:
        :return: if unlock variable succeed or not
        """
        lock = self.lock_table.get(variable_id, None)

        if lock is None:
            raise KeyError(f"Try to unlock the variable ({variable_id}) which has no lock on it")

        unlock_counts = 0

        if transaction_id in self.lock_table[variable_id][0]:
            self.lock_table[variable_id][0].remove(transaction_id)
            unlock_counts += 1

        if transaction_id == self.lock_table[variable_id][1]:
            self.lock_table[variable_id][1] = None
            unlock_counts += 1

        # There is only one lock on given variable for the given transaction,
        # we need to make sure this is true, if there is a bug, this will work
        assert unlock_counts == 1

    def clear(self):
        """
        Clear the lock table, when site fail, we should clear locks
        :return: None
        """
        self.lock_table = {}

