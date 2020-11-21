from model.Operation import *


class WaitFor(object):
    """
    TODO: implement WaitFor algorithm
    """
    def __init__(self, tm):
        self.var_to_ops = {}
        self.tm = tm
        self.wait_for = {}
        self.trace = []

    def add_operation(self, operation):
        op_t = operation.get_op_t()
        para = operation.get_parameters()

        # Only process read and write operations
        if not (op_t == "R" or op_t == "W"):
            return

        trans_id, var_id = para[0], para[1]

        # ignore the operation belongs to a readonly transaction
        if self.tm.transactions[trans_id].is_readonly:
            return

        # Get all operations on the variable
        ops = self.var_to_ops.get(var_id, set())

        if op_t == "R":
            for op in ops:
                if op.get_op_t() == "W" and op.get_parameters()[0] != trans_id:
                    waits = self.wait_for.get(trans_id, set())
                    waits.add(op.get_parameters()[0])
                    self.wait_for[trans_id] = waits
        else:
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
        nodes = list(self.wait_for.keys())
        self.trace = []

        for target in nodes:
            visited = {node: False for node in nodes}
            if self._recursive_check(target, target, visited, self.trace):
                return True
        return False

    def get_trace(self):
        return self.trace

    def remove_transaction(self, transaction_id):
        # Modify var_to_trans
        for var, ops in self.var_to_ops.items():
            ops = {op for op in ops if op.get_parameters()[0] != transaction_id}
            self.var_to_ops[var] = ops

        # Modify wait for
        self.wait_for.pop(transaction_id, None)




