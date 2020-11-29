from prettytable import PrettyTable


class Operation(object):
    """
    This class abstract out all kinds of operation sent by user.
    The subclass including: Begin, BeginRO, Read, Write, End, Fail, Recover
    The logic of operation is embedded in the function of "execute(tick: int, tm: TransactionManager, retry: bool)"
    """
    def __init__(self, para: [str]):
        self.op_t = None  # operation type
        self.para = para  # parameters of the operation

    def __str__(self):
        return f"{self.op_t}({','.join(self.para)})"

    def execute(self, tick: int, tm, retry=False):
        """
        Execute the operation if it is feasible

        :param tick: time
        :param tm: Transaction Manager
        :param retry: whether this execution is a retry
        :return: bool, indicate whether this execution is succeed
        """
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
        tm.wait_for_graph.add_operation(self)

    def get_parameters(self):
        return self.para

    def get_op_t(self):
        return self.op_t


def parse_variable_id(variable_id):
    """
    Split variable id, for example, from "x12" to ("x" and "12")

    :param variable_id: variable id string
    :return: A tuple
    """
    for idx, c in enumerate(variable_id):
        if c.isdigit():
            return variable_id[:idx], int(variable_id[idx:])


def print_result(headers, rows):
    """
    Print the query result using pretty table, only for dump operation now

    :param headers: table headers
    :param rows: table rows
    :return: None
    """
    table = PrettyTable()
    table.field_names = headers
    for row in rows:
        table.add_row(row)
    print(table)


# Read variable from log if the variable was modified by transaction,
# otherwise read from committed data
def do_read(trans_id, var_id, site):
    """
    Read the variable, and print in prettytable

    :param trans_id: transaction id
    :param var_id: variable id
    :param site: site
    :return:
    """

    # Case 1: data has been changed by the same transaction but not commit before
    if trans_id in site.data_manager.log and var_id in site.data_manager.log[trans_id]:
        res = site.data_manager.log[trans_id][var_id]
    else:
        res = site.data_manager.get_variable(var_id)

    print_result(["Transaction", "Site", f"x{var_id}"], [[trans_id, f"{site.site_id}", res]])

    return True
