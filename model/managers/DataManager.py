from configurations import distinct_variable_counts, number_of_sites


class DataManager(object):
    @staticmethod
    def _init_db(idx):
        data = [None] * distinct_variable_counts
        for i in range(1, distinct_variable_counts + 1):
            if i % 2 == 0 or i % number_of_sites + 1 == idx:
                data[i - 1] = 10 * i
        return data

    def __init__(self, site_id):
        self.site_id = site_id
        self._data = self._init_db(site_id)

    # return values for all variable to prettyTable (which will be printed in dump operation)
    def echo(self):
        prefix = f"Site {self.site_id}"
        return [prefix] + [v for v in self._data]