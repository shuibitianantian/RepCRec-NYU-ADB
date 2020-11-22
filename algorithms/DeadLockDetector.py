
class WaitFor(object):
    """
    Implementation of wait for graph
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
        Add operation to self.var_to_ops
        Add new transaction node in wait-for graph
        :param operation:
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
        :return: if there is a deadlock
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
        Return all transaction in the circle, call this only when check_deadlock is true
        :return: all transactions in the circle
        """
        return self.trace

    def remove_transaction(self, transaction_id):
        # Modify var_to_trans
        for var, ops in self.var_to_ops.items():
            ops = {op for op in ops if op.get_parameters()[0] != transaction_id}
            self.var_to_ops[var] = ops

        # Modify wait for graph, delete the node of given transaction id
        self.wait_for.pop(transaction_id, None)




