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

    for tick, op in enumerate(case):
        op_t, para = OperationParser.parse(op)
        operation = OperationCreator.create(op_t, para)
        tm.step(operation, tick)

    while tm.blocked:
        tm.retry()