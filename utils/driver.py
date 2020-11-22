from configurations import *
from model.Site import Site
from model.managers.TransactionManager import TransactionManager
from model.Operation import OperationParser, OperationCreator


def init_sites():
    """
    Initialize sites and return list of sites
    :return: list of sites
    """
    return [Site(idx) for idx in range(1, number_of_sites + 1)]


def run(case):
    """
    run RepCRec algorithm on a list of operations (single test case)
    :param case: a list of operations
    :return: None
    """
    tm = TransactionManager()
    tm.attach_sites(init_sites())

    tick = 0
    for op in case:
        tick += 1
        op_t, para = OperationParser.parse(op)
        operation = OperationCreator.create(op_t, para)
        tm.step(operation, tick)

    while tm.blocked:
        cur_blocked_size = len(tm.blocked)
        tick += 1
        tm.retry(tick)

        if cur_blocked_size == len(tm.blocked):
            print("Following operation can not be executed, maybe the test case is not terminable:")
            for op in tm.blocked:
                print(op)
            break
