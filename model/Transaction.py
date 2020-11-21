
class Transaction(object):
    def __init__(self, identifier, tick, is_readonly=False):
        self.transaction_id = identifier
        self.is_readonly = is_readonly

        # Save operations of the transaction
        self.operations = []

        # whether to be aborted when commit, only for transactions which operate in the sites have failed then recovered
        self.to_be_aborted = False

        # Start time of this transaction
        self.tick = tick

    def add_operation(self, operation):
        """
        Add given operation to the transactions
        :param operation: an operation
        :return: None
        """
        self.operations.append(operation)

    def __str__(self):
        return f"Identifier: {self.transaction_id} & ReadOnly: {self.is_readonly} & Operations: {[str(op) for op in self.operations]}"

