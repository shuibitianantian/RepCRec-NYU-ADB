
class WaitFor(object):
    """
    A simple implementation of wait-for graph for deadlock detection

    :param self.tm: TransactionManager
    :param self.var_to_ops: A dictionary mapping variable id to a set of operations which want to access the variable
    :param self.wait_for: A dictionary tracking the wait-for edges, key is the from point, value is a set of transactions
    :param self.trace: A list contains a cycle in the wait-for graph if any circle exists
    """

    def __init__(self, tm):
        self.tm = tm
        # Key-Value pair, tracking the operations that access each variable, (variable id: set of operation)
        # if a variable id does not exist, then no transaction access this variable
        self.var_to_ops = {}

        # key value pair, tracking the wait-for-edges, (trans_id: set of transaction)
        self.wait_for = {}

        # a list to track the circle in current execution,
        # will be used to find the youngest transaction in the circle
        self.trace = []

    def add_operation(self, operation):
        """
        Add operation to self.var_to_ops dictionary, for example, if the operation want to access x1,
        we add the operation in this way self.var_to_ops["x1"].add(operation)

        Add new node in wait-for graph if the transactions does not exist

        ReadOnly operation will be ignored

        :param operation: Operation object
        :return: None
        """
        op_t = operation.get_op_t()
        para = operation.get_parameters()

        # Only add read and write operations
        if not (op_t == "R" or op_t == "W"):
            return

        trans_id, var_id = para[0], para[1]

        # ignore the operation belongs to a readonly transaction
        if self.tm.transactions[trans_id].is_readonly:
            return

        # Get all operations on the variable
        ops = self.var_to_ops.get(var_id, set())

        # Case 1: operation is R
        if op_t == "R":
            # Check if previous operation of the same transaction operated on the same variable
            # if so, no deadlock will be formed by adding this operation
            for op in ops:
                if op.get_parameters()[0] == trans_id:
                    # Add operation to the dictionary
                    ops.add(operation)
                    self.var_to_ops[var_id] = ops
                    return

            # for any operation which is on the same variable,
            # if op is W and transaction id is different, then there should be a edge
            # For example, op is W(T1, x1, 10), the operation to be added is R(T2, x1)
            # then the edge is T2 -> T1
            for op in ops:
                if op.get_op_t() == "W" and op.get_parameters()[0] != trans_id:
                    waits = self.wait_for.get(trans_id, set())
                    waits.add(op.get_parameters()[0])
                    self.wait_for[trans_id] = waits
        # Case 2: operation is W
        else:
            # Check if previous operation of the same transaction operated on the same variable
            # if so, no deadlock will be formed by adding this operation
            for op in ops:
                if op.get_parameters()[0] == trans_id and op.get_op_t() == "W":
                    # Add operation to the dictionary
                    ops.add(operation)
                    self.var_to_ops[var_id] = ops
                    return

            # W operation will conflict with all other operation on the same variable
            for op in ops:
                if op.get_parameters()[0] != trans_id:
                    waits = self.wait_for.get(trans_id, set())
                    waits.add(op.get_parameters()[0])
                    self.wait_for[trans_id] = waits

        # Add operation to the dictionary
        ops.add(operation)
        self.var_to_ops[var_id] = ops

    def _recursive_check(self, cur_node, target, visited, trace):
        visited[cur_node] = True

        if cur_node not in self.wait_for:
            return False

        trace.append(cur_node)
        neighbor_nodes = self.wait_for[cur_node]

        for neighbor in neighbor_nodes:
            if neighbor == target:
                return True
            elif neighbor not in visited:
                continue
            elif not visited[neighbor]:
                if self._recursive_check(neighbor, target, visited, trace):
                    return True

        trace.pop(-1)
        return False

    def check_deadlock(self):
        """
        Detect if there is a circle in current execution

        :return: True if there is a deadlock, otherwise False
        """
        nodes = list(self.wait_for.keys())
        self.trace = []

        for target in nodes:
            visited = {node: False for node in nodes}
            if self._recursive_check(target, target, visited, self.trace):
                return True
        return False

    def get_trace(self):
        """
        Return all transaction in a deadlock circle

        Note: This function will return a empty list if self.check_deadlock() == True

        :return: A list of transaction
        """
        return self.trace

    def remove_transaction(self, transaction_id):
        """
        Remove wait-for node has the transaction_id, remove all operations belong to the transaction

        Typically, this function will be called when a transaction has been aborted or has committed

        :param transaction_id: identifier of the transaction
        :return: None
        """
        # Modify var_to_trans
        for var, ops in self.var_to_ops.items():
            ops = {op for op in ops if op.get_parameters()[0] != transaction_id}
            self.var_to_ops[var] = ops

        # Modify wait for graph, delete the node of given transaction id
        self.wait_for.pop(transaction_id, None)




