import re
from utils.FileLoader import FileLoader
from model.Transaction import Transaction
from configurations import distinct_variable_counts
from prettytable import PrettyTable
from configurations import *

TABLE_HEADERS = ["Site Name"] + [f"x{i}" for i in range(1, distinct_variable_counts + 1)]


# split variable id like "x12" to "x" and "12"
def parse_variable_id(variable_id):
    for idx, c in enumerate(variable_id):
        if c.isdigit():
            return variable_id[:idx], int(variable_id[idx:])


def print_result(headers, rows):
    table = PrettyTable()
    table.field_names = headers
    for row in rows:
        table.add_row(row)
    print(table)


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


class Operation(object):
    def __init__(self, para: [str]):
        self.op_t = None  # operation type
        self.para = para  # parameters of the operation

    def __str__(self):
        return f"{self.op_t}({','.join(self.para)})"

    def execute(self, tick: int, tm):
        pass

    def save_to_transaction(self, tm):
        """
        Append operation to corresponding transaction's operation list
        :param tm: Transaction Manager
        :return: None
        """
        if len(self.para) == 0:
            raise TypeError("Try to append dump operation to transaction")

        transaction_id = self.para[0]
        if transaction_id not in tm.transactions:
            raise KeyError(f"Try to execute {self.op_t} in a non-existing transaction")

        tm.transactions[transaction_id].add_operation(self)


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
                site = tm.sites[var_id % number_of_sites]
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
            site = tm.sites[var_id % number_of_sites]
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
        table = PrettyTable()
        table.field_names = TABLE_HEADERS
        for site in tm.sites:
            table.add_row(site.data_manager.echo())
        print(table)


class Fail(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "fail")

    def execute(self, tick: int, tm):
        """
        Convert specific site to fail
        :param tick:
        :param tm:
        :return:
        """
        site_id = int(self.para[0])
        tm.sites[site_id].fail()


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
        tm.sites[site_id].recover()


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
if __name__ == "__main__":
    loader = FileLoader("../TestFiles/test.txt")
    case1 = loader.next_case()

    for op in case1:
        op_t, para = OperationParser.parse(op)
        print(OperationCreator.create(op_t, para))
