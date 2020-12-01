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
    Run RepCRec algorithm on a list of operations (single test case), the result will be saved in the stdout

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


def run_interactive():
    """
    Run the interactive mode which allows user to enter operations line by line

    :return: None
    """
    # Initialized Transaction manager and sites
    tm = TransactionManager()
    tm.attach_sites(init_sites())
    tick = 0

    while True:
        command = input("RepCRec >: ")
        try:
            if command == "refresh":
                tm = TransactionManager()
                tm.attach_sites(init_sites())
                tick = 0
            elif command == "<END>":
                while tm.blocked:
                    cur_blocked_size = len(tm.blocked)
                    tick += 1
                    tm.retry(tick)

                    if cur_blocked_size == len(tm.blocked):
                        print("Following operation can not be executed, maybe the test case is not terminable:")
                        for op in tm.blocked:
                            print(op)
                        break

                tm = TransactionManager()
                tm.attach_sites(init_sites())
                tick = 0

            elif command == "quit":
                print("bye")
                break
            else:
                tick += 1
                op_t, para = OperationParser.parse(command)
                operation = OperationCreator.create(op_t, para)
                try:
                    tm.step(operation, tick)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

