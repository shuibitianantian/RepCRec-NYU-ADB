import re
from . import Operation, print_result, parse_variable_id
from utils.FileLoader import FileLoader
from model.Transaction import Transaction
from configurations import *

TABLE_HEADERS = ["Site Name"] + [f"x{i}" for i in range(1, distinct_variable_counts + 1)]


class OperationParser(object):
    reg_pattern = r"(.*)\((.*?)\)"

    @staticmethod
    def parse(line):
        """
        Extract operation type and parameters out from a textual parameter
        :param line: A textual operation, for example "begin(T1)"
        :return: (operation type, parameters)
        """
        res = re.search(OperationParser.reg_pattern, line)
        return res.group(1), res.group(2).split(",")


class Begin(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "begin")

    def execute(self, tick, tm):
        """
        Execute an operation, for Begin, we just need to initialize a new transaction in Transaction Manager
        :param tick: time
        :param tm: Transaction Manager
        :return: None
        """
        trans = Transaction(self.para[0], tick)
        if trans.transaction_id in tm.transactions:
            raise KeyError(f"Dupilcated transaction {trans.transaction_id}")
        else:
            tm.transactions[trans.transaction_id] = trans
        return True


class BeginRO(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "beginRO")

    def execute(self, tick, tm):
        """
        Initialize a readonly transaction in Transaction Manager
        :param tick: time
        :param tm: Transaction Manager
        :return: None
        """
        trans = Transaction(self.para[0], tick, True)

        if trans.transaction_id in tm.transactions:
            raise KeyError(f"Dupilcated transaction {trans.transaction_id}")
        else:
            tm.transactions[trans.transaction_id] = trans
            # take snapshot for each site at current tick
            for site in tm.sites:
                # only take snapshot when site is up
                if site.up:
                    site.snapshot(tick)

        return True


class Read(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "R")

    def execute(self, tick: int, tm):
        """
        TODO: Need bugfix
        :param tick: time
        :param tm: Transaction Manager
        :return:
        """
        self.save_to_transaction(tm)
        trans_id, var_id_str = self.para[0], self.para[1]
        _, var_id = parse_variable_id(var_id_str)
        # Case 1: read_only transaction
        if tm.transactions[trans_id].is_readonly:
            trans_start_tick = tm.transactions[trans_id].tick
            # Situation 1.1: if the index of variable read is odd, then we just need to check specific site
            if var_id % 2 != 0:
                site = tm.get_site(var_id % number_of_sites + 1)
                if not site.up:
                    return False
                else:
                    headers = ["Transaction", "Site", var_id_str]
                    # Only one row here
                    rows = [[trans_id, f"{site.site_id}", f"{site.get_snapshot_variable(trans_start_tick, var_id)}"]]
                    print_result(headers, rows)
                    return True
            # Situation 1.2: if the index of variable read is even, we just check the first available site to read
            else:
                for site in tm.sites:
                    if not site.up:
                        continue
                    else:
                        headers = ["Transaction", "Site", var_id_str]
                        # Only one row here
                        rows = [[trans_id, f"{site.site_id}", f"{site.get_snapshot_variable(trans_start_tick, var_id)}"]]
                        print_result(headers, rows)
                        return True
        # Case 2: typical transaction and the index of variable read is odd, then we just need to check specific site
        elif var_id % 2 != 0:
            site = tm.get_site(var_id % number_of_sites + 1)
            if not site.up:
                return False
            elif site.data_manager.check_accessibility(var_id):
                succeed = site.lock_manager.try_lock_variable(trans_id, var_id, 0)
                if succeed:
                    print_result(["Transaction", "Site", var_id_str],
                                 [[trans_id, f"{site.site_id}", f"{site.data_manager.get_variable(var_id)}"]])
                    return True
                else:
                    return False
        # Case 3: typical transaction and the index of variable read is even,
        # we just check the first available site to read
        else:
            for site in tm.sites:
                if not site.up:
                    continue
                elif site.data_manager.check_accessibility(var_id):
                    succeed = site.lock_manager.try_lock_variable(trans_id, var_id, 0)
                    if succeed:
                        print_result(["Transaction", "Site", var_id_str],
                                     [[trans_id, f"{site.site_id}", f"{site.data_manager.get_variable(var_id)}"]])
                        return True
        return False


class Write(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "W")

    def execute(self, tick: int, tm):
        """
        TODO: Add logic here
        :param tick: time
        :param tm: Transaction Manager
        :return:
        """
        self.save_to_transaction(tm)
        trans_id, var_id_str, write_value = self.para[0], self.para[1], self.para[2]
        _, var_id = parse_variable_id(var_id_str)

        # Case 1: variable id is odd,
        if var_id % 2 != 0:
            site = tm.get_site(var_id % number_of_sites + 1)

            if not site.up:
                print("Site is down, can not get the variable")
                return False
            elif site.lock_manager.try_lock_variable(trans_id, var_id_str, 1):
                logs = site.data_manager.log.get(trans_id, {})
                logs[var_id] = write_value
                site.data_manager.log[trans_id] = logs
                return True
            else:
                print("Site is up, but can not get the lock")
                return False
        # Case 2: variable id is even, need to get locks of all available sites
        else:
            locked_sites = []
            # Try to lock all sites have given variable
            for site in tm.sites:
                # ignore fail sites
                if not site.up:
                    continue
                # try to lock the variable in health site and append it to locked sites
                elif site.lock_manager.try_lock_variable(trans_id, var_id_str, 1):
                    locked_sites.append(site)
                # if lock conflicting in any health site, release all locks added previously
                else:
                    for locked_site in locked_sites:
                        locked_site.try_unlock_variable(var_id_str, trans_id)
                    print("Can not get all exclusive locks for a site-wide variable")
                    return False

            if len(locked_sites) == 0:
                return False

            # At this point, we can guarantee that program has got all necessary locks for the write operation
            # Perform write operation on all locked sites
            for locked_site in locked_sites:
                logs = locked_site.data_manager.log.get(trans_id, {})
                logs[var_id] = write_value
                locked_site.data_manager.log[trans_id] = logs

            return True


class Dump(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "dump")

    def execute(self, tick: int, tm):
        """
        Print out all variables of each site
        :param tick: time
        :param tm: Transaction Manager
        :return: None
        """
        rows = [site.data_manager.echo() for site in tm.sites]
        print_result(TABLE_HEADERS, rows)
        return True


class Fail(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "fail")

    def execute(self, tick: int, tm):
        """
        Convert specific site to fail
        :param tick: time
        :param tm:
        :return:
        """
        site_id = int(self.para[0])
        tm.get_site(site_id).fail()
        return True


class Recover(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "recover")

    def execute(self, tick: int, tm):
        """
        Convert specific site to up
        :param tick: time
        :param tm: Transaction Manager
        :return: None
        """
        site_id = int(self.para[0])
        tm.get_site(site_id).recover()
        return True


class End(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "end")

    def execute(self, tick: int, tm):
        """
        TODO: Commit transaction
        :param tick: time
        :param tm: Transaction Manager
        :return: None
        """
        self.save_to_transaction(tm)

        # commit all changes made by given transaction
        trans_id = self.para[0]
        print(f"Transaction {trans_id} commit")

        for site in tm.sites:
            # Check if the site has changed by the given transaction
            if site.up and trans_id in site.data_manager.log:
                change_logs = site.data_manager.log[trans_id]
                for var_id, val in change_logs.items():
                    site.data_manager.set_variable(var_id, val)
                    site.data_manager.is_accessible[var_id - 1] = True
                # delete the change, because commit
                site.data_manager.log.pop(trans_id)

            site.lock_manager.release_transaction_locks(trans_id)

        return True


class OperationCreator(object):
    types = {
        "begin": Begin,
        "W": Write,
        "R": Read,
        "dump": Dump,
        "beginRO": BeginRO,
        "end": End,
        "fail": Fail,
        "recover": Recover
    }

    @staticmethod
    def create(op_t, para):
        if op_t not in OperationCreator.types:
            raise KeyError("Unknown Operation Type")
        return OperationCreator.types[op_t](para)


# Following code is for test
# if __name__ == "__main__":
#     loader = FileLoader("../TestFiles/test.txt")
#     case1 = loader.next_case()
#
#     for op in case1:
#         op_t, para = OperationParser.parse(op)
#         print(OperationCreator.create(op_t, para))
