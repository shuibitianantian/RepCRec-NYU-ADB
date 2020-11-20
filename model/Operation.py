import re
from utils.FileLoader import FileLoader
from model.Transaction import Transaction
from configurations import distinct_variable_counts
from prettytable import PrettyTable

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
        TODO: add logic here
        :param tick: time
        :param tm: Transaction Manager
        :return:
        """
        self.save_to_transaction(tm)


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
        :return:
        """
        trans = Transaction(self.para[0], tick, True)

        if trans.transaction_id in tm.transactions:
            raise KeyError(f"Dupilcated transaction {trans.transaction_id}")
        else:
            tm.transactions[trans.transaction_id] = trans


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





