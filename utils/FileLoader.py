
class FileLoader(object):
    def __init__(self, file_name):
        self._lines = []

        with open(file_name, 'r') as f:
            for line in f.readlines():
                if not line.startswith("//"):  # ignore comments
                    self._lines.append(line.strip())

        self._end_idx = len(self._lines) - 1
        self._buffer_idx = -1

    def next_case(self):

        operations = []
        for idx, line in enumerate(self._lines[self._buffer_idx + 1:]):
            self._buffer_idx += 1
            if not line.startswith('//') and line.strip() != "<END>" and line.strip() != '':
                operations.append(line.strip())
            if line.strip() == "<END>":
                break

        return operations

    def has_next(self):
        return self._buffer_idx < self._end_idx

