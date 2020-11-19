from utils.FileLoader import FileLoader
from configurations import *
from model.managers.TransactionManager import TransactionManager
from model.Site import Site
from model.Operation import OperationParser, OperationCreator


def init_sites():
    return [Site(idx) for idx in range(1, number_of_sites + 1)]


def solve(case):
    tm = TransactionManager()
    tm.attach_sites(init_sites())

    for tick, op in enumerate(case):
        op_t, para = OperationParser.parse(op)
        operation = OperationCreator.create(op_t, para)
        tm.step(operation, tick)

    while tm.blocked:
        tm.retry()


if __name__ == "__main__":
    loader = FileLoader("./TestFiles/test.txt")

    case_id = 1
    while loader.has_next():
        print(f"Test {case_id} Result")
        case = loader.next_case()
        solve(case)
        case_id += 1