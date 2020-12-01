import re
from . import Operation, print_result, parse_variable_id, do_read
from model.Transaction import Transaction
from configurations import *

TABLE_HEADERS = ["Site Name"] + [f"x{i}" for i in range(1, distinct_variable_counts + 1)]


class OperationParser(object):
    reg_pattern = r"(.*)\((.*?)\)"

    @staticmethod
    def parse(line):
        """
        Extract operation type and parameters out from a textual parameter

        :param line: A textual operation, for example "begin(T1)"
        :return: (operation type, parameters)
        """
        res = re.search(OperationParser.reg_pattern, line)
        return res.group(1), [s.strip() for s in res.group(2).split(",")]


class Begin(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "begin")

    def execute(self, tick, tm, retry=False):
        """
        Execute an operation, for Begin, we just need to initialize a new transaction in Transaction Manager

        :param retry: if the operation is a retry
        :param tick: time
        :param tm: Transaction Manager
        :return: True
        """
        trans = Transaction(self.para[0], tick)
        if trans.transaction_id in tm.transactions:
            raise KeyError(f"Dupilcated transaction {trans.transaction_id}")
        else:
            tm.transactions[trans.transaction_id] = trans
        return True


class BeginRO(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "beginRO")

    def execute(self, tick, tm, retry=False):
        """
        Initialize a readonly transaction in Transaction Manager

        :param retry: If the operation is a retry
        :param tick: time
        :param tm: Transaction Manager
        :return: True
        """
        trans = Transaction(self.para[0], tick, True)

        if trans.transaction_id in tm.transactions:
            raise KeyError(f"Dupilcated transaction {trans.transaction_id}")
        else:
            tm.transactions[trans.transaction_id] = trans
            # take snapshot for each site at current tick
            for site in tm.sites:
                # only take snapshot when site is up

                # All sites will take snapshot, but only the variable is accessible will be included in the
                # snapshot which means for fail site, only non replicated data will be included
                # for up sites all accessible variable will be included in the snapshot
                site.snapshot(tick)

        return True


class Read(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "R")

    def execute(self, tick: int, tm, retry=False):
        """
        Execute the read operation, for both read and readonly

        :param retry: If the transaction is a retry
        :param tick: time
        :param tm: Transaction Manager
        :return: True or False
        """
        if not retry:
            self.save_to_transaction(tm)

        trans_id, var_id_str = self.para[0], self.para[1]
        _, var_id = parse_variable_id(var_id_str)
        # Case 1: read_only transaction
        if tm.transactions[trans_id].is_readonly:
            trans_start_tick = tm.transactions[trans_id].tick
            # Situation 1.1: if the index of variable read is odd, then we just need to check specific site
            # we do not abort the transaction because we know this variable can only be accessed by one site,
            # if the site is down, we just need to wait it recover and we can get the value
            if var_id % 2 != 0:
                site = tm.get_site(var_id % number_of_sites + 1)
                if not site.up:
                    return False
                elif var_id in site.snapshots[trans_start_tick] and var_id in site.snapshots[trans_start_tick]:
                    headers = ["Transaction", "Site", var_id_str]
                    # Only one row here
                    rows = [[trans_id, f"{site.site_id}", f"{site.get_snapshot_variable(trans_start_tick, var_id)}"]]
                    print_result(headers, rows)
                    return True

            # Situation 1.2: if the index of variable read is even, we check the first available site to read
            # if we can not access the variable from all up sites, abort the read-only transaction,
            # by the definition:
            #       If xi is replicated then RO can read xi from site s if xi was committed
            #       at s before RO began and s was up all the time between the time when
            #       xi was commited and RO began.
            # This indicates we can not read any value (if the value had not been changed and committed by a transaction
            # after the site recovered) from a site fail and recover before the RO transaction began, any value had been
            # changed and committed by a transaction will be accessible, the logic is coded in site.snapshot and
            # site.fail.
            # Note: if we could not access the replicated value from all up sites, then we abort the transaction,
            # because even the site with the latest committed value recover later than RO began,
            # by definition above and how we read data:
            #           (Upon recovery of a site s, all non-replicated variables are available for reads and
            #            writes. Regarding replicated variables, the site makes them available for writing,
            #            but not reading.)
            # We could know, the value in the site is not readable unless some transaction changed it and committed
            # that value, but this break the definition of multi-version read consistency, so we abort the readonly
            # transaction.
            else:
                has = False
                for site in tm.sites:
                    # if the site has the variable is down, has -> True, we could retry latter
                    if not site.up and trans_start_tick in site.snapshots and var_id in site.snapshots[trans_start_tick]:
                        has = True
                    elif trans_start_tick in site.snapshots and var_id in site.snapshots[trans_start_tick]:
                        headers = ["Transaction", "Site", var_id_str]
                        # Only one row here
                        rows = [[trans_id, f"{site.site_id}", f"{site.get_snapshot_variable(trans_start_tick, var_id)}"]]
                        print_result(headers, rows)
                        return True
                # No site has the variable in the snapshot
                if not has:
                    tm.abort(trans_id, 3)
                    return True
        # Case 2: typical transaction and the index of variable read is odd, then we just need to check specific site
        elif var_id % 2 != 0:
            site = tm.get_site(var_id % number_of_sites + 1)
            if not site.up:
                return False
            elif site.data_manager.check_accessibility(var_id):
                if site.lock_manager.try_lock_variable(trans_id, var_id_str, 0):
                    return do_read(trans_id, var_id, site)
                else:
                    return False
        # Case 3: typical transaction and the index of variable read is even,
        else:
            for site in tm.sites:
                if not site.up:
                    continue
                elif site.data_manager.check_accessibility(var_id):
                    if site.lock_manager.try_lock_variable(trans_id, var_id_str, 0):
                        return do_read(trans_id, var_id, site)
        return False


class Write(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "W")

    def execute(self, tick: int, tm, retry=False):
        """
        Execute Write operation

        :param retry: If the operation is a retry
        :param tick: time
        :param tm: Transaction Manager
        :return: True or False
        """
        if not retry:
            self.save_to_transaction(tm)

        trans_id, var_id_str, write_value = self.para[0], self.para[1], int(self.para[2])
        _, var_id = parse_variable_id(var_id_str)

        # Case 1: variable id is odd
        if var_id % 2 != 0:
            site = tm.get_site(var_id % number_of_sites + 1)
            # Situation 1.1: Site failed, return false
            if not site.up:
                # print(f"Site {site.site_id} is down, {self}")
                return False
            # Situation 1.2: Site up and lock variable succeed, return true
            elif site.lock_manager.try_lock_variable(trans_id, var_id_str, 1):
                logs = site.data_manager.log.get(trans_id, {})
                logs[var_id] = write_value
                site.data_manager.log[trans_id] = logs
                return True
            # Situation 1.3: Site up, but lock variable failed, return false
            else:
                # print(f"Site {site.site_id} is up, but can not get the lock, {self}")
                return False
        # Case 2: variable id is even, need to get locks of all available sites
        else:
            locked_sites = []
            # Try to lock all sites have given variable
            for site in tm.sites:
                # ignore fail sites
                if not site.up:
                    continue
                # try to lock the variable in health site and append it to locked sites
                elif site.lock_manager.try_lock_variable(trans_id, var_id_str, 1):
                    locked_sites.append(site)
                # if lock conflicting in any health site, release all locks added previously
                else:
                    for locked_site in locked_sites:
                        locked_site.try_unlock_variable(var_id_str, trans_id)
                    # print(f"Can not get all exclusive locks for a site-wide variable, {self}")
                    return False

            if len(locked_sites) == 0:
                # print("No site available now, retry later")
                return False

            # At this point, we can guarantee that program has got all necessary locks for the write operation
            # Perform write operation on all locked sites
            for locked_site in locked_sites:
                logs = locked_site.data_manager.log.get(trans_id, {})
                logs[var_id] = write_value
                locked_site.data_manager.log[trans_id] = logs

            return True


class Dump(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "dump")

    def execute(self, tick: int, tm, retry=False):
        """
        Print out all variables of each site

        :param retry: indicate this is a retry operation, even though Dump would not be retried, we add this parameter for consistency
        :param tick: time
        :param tm: Transaction Manager
        :return: True
        """
        rows = [site.echo() for site in tm.sites]
        print_result(TABLE_HEADERS, rows)
        return True


class Fail(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "fail")

    def execute(self, tick: int, tm, retry=False):
        """
        Convert specific site to fail

        :param retry: If the operation is a retry, Fail operation will not be retried
        :param tick: time
        :param tm: Transaction Manager
        :return: True
        """
        site_id = int(self.para[0])
        site = tm.get_site(site_id)
        transactions = site.lock_manager.get_involved_transactions()

        # Flag transaction to be aborted when commit
        for trans_id in transactions:
            tm.transactions[trans_id].to_be_aborted = True
        site.fail()
        return True


class Recover(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "recover")

    def execute(self, tick: int, tm, retry=False):
        """
        Convert specific site to up
        :param retry: If the operation is a retry, Recover will not be retried
        :param tick: time
        :param tm: Transaction Manager
        :return: True
        """
        site_id = int(self.para[0])
        tm.get_site(site_id).recover()
        return True


class End(Operation):
    def __init__(self, para):
        super().__init__(para)
        Operation.__setattr__(self, "op_t", "end")

    def execute(self, tick: int, tm, retry=False):
        """
        Commit change of the transaction

        :param retry: If the operation is a retry
        :param tick: time
        :param tm: Transaction Manager
        :return: True
        """
        if not retry:
            self.save_to_transaction(tm)

        # If a transaction T accesses an item (really accesses it, not just request
        # a lock) at a site and the site then fails, then T should continue to execute
        # and then abort only at its commit time (unless T is aborted earlier due to
        # deadlock).
        if tm.transactions[self.para[0]].to_be_aborted:
            tm.abort(self.para[0], 1)
            return True

        # commit all changes made by given transaction
        trans_id = self.para[0]
        trans_start_time = tm.transactions[trans_id].tick

        # If there are blocked operation of the commit transaction, block the commit
        if trans_id in tm.blocked_transactions:
            return False

        print(f"Transaction {trans_id} commit")

        for site in tm.sites:
            # Check if the site has changed by the given transaction
            if site.up and trans_id in site.data_manager.log:
                change_logs = site.data_manager.log[trans_id]
                for var_id, val in change_logs.items():
                    site.data_manager.set_variable(var_id, val)
                    site.data_manager.is_accessible[var_id - 1] = True
                # delete the change, because commit
                site.data_manager.log.pop(trans_id)
            # when readonly transaction end, delete the snapshot belongs to it
            elif site.up and trans_start_time in site.snapshots:
                site.snapshots.pop(trans_start_time)

            site.lock_manager.release_transaction_locks(trans_id)

        # When transaction commit, we need to remove the transaction in the wait for graph
        tm.wait_for_graph.remove_transaction(trans_id)

        return True


class OperationCreator(object):
    types = {
        "begin": Begin,
        "W": Write,
        "R": Read,
        "dump": Dump,
        "beginRO": BeginRO,
        "end": End,
        "fail": Fail,
        "recover": Recover
    }

    @staticmethod
    def create(op_t, para):
        """
        Create an operation object based on the opeartion type and its parameters

        :param op_t: operation type
        :param para: parameters of operation
        """

        if op_t not in OperationCreator.types:
            raise KeyError("Unknown Operation Type")
        return OperationCreator.types[op_t](para)
