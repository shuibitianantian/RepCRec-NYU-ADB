from prettytable import PrettyTable


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
        tm.wait_for_graph.add_operation(self)

    def get_parameters(self):
        return self.para

    def get_op_t(self):
        return self.op_t


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


# Read variable from log if modified by transaction,
# otherwise read from committed data
def do_read(trans_id, var_id, site):
    if trans_id in site.data_manager.log and var_id in site.data_manager.log[trans_id]:
        res = site.data_manager.log[trans_id][var_id]
    else:
        res = site.data_manager.get_variable(var_id)

    print_result(["Transaction", "Site", f"x{var_id}"], [[trans_id, f"{site.site_id}", res]])

    return True