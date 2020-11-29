
class FileLoader(object):
    """
    FileLoader is used to open test file and extract cases and operations
    """
    def __init__(self, file_name):
        self._lines = []

        with open(file_name, 'r') as f:
            for line in f.readlines():
                if not line.startswith("//"):  # ignore comments
                    self._lines.append(line.strip())

        self._end_idx = len(self._lines) - 1
        self._buffer_idx = -1

    def next_case(self):
        """
        Gather all operations of next test case

        :return: list of operations
        """
        operations = []
        for idx, line in enumerate(self._lines[self._buffer_idx + 1:]):
            self._buffer_idx += 1
            if not line.startswith('//') and line.strip() != "<END>" and line.strip() != '':
                operations.append(line.strip())
            if line.strip() == "<END>":
                break

        return operations

    def has_next(self):
        """
        Check if there are remaining test cases

        :return: True or False
        """
        return self._buffer_idx < self._end_idx

