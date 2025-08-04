import configparser
import logging
from collections import defaultdict

config = configparser.ConfigParser()
config.read('config.ini')

class InMemoryDB:
    def __init__(self):
        """
        Database initialization method

        _main_db - main data store
        _counts - counting the number of values
        _transaction_stack - transaction storage stack
        __MAX_TRANSACTION_DEPTH - maximum transaction depth
        __MAX_DB_SIZE - maximum database size
        logger - logger for audit
        """
        self._main_db = {}
        self._counts = defaultdict(int)
        self._transaction_stack = []
        self.__MAX_TRANSACTION_DEPTH = config.getint('DEFAULT', 'MAX_TRANSACTION_DEPTH', fallback=100)
        self.__MAX_DB_SIZE = config.getint('DEFAULT', 'MAX_DB_SIZE', fallback=100000)

        logging.basicConfig(
            filename=config.get('DEFAULT', 'LOG_FILE', fallback='db_logs'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("InMemoryDB")

    @property
    def db_size(self):
        """
        The method returns the current size of the database
        (read-only)
        """
        return len(self._main_db)

    @property
    def transaction_depth(self):
        """
        The method returns the current transaction depth
        (read-only)
        """
        return len(self._transaction_stack)

    def _validate_key_value(self, key: str, value: str):
        """
        Method for validating input parameters
        """
        if not (isinstance(key, str) and key.isalnum()):
            raise ValueError("Key must be alphanumeric string")
        if not (isinstance(value, str) and value.isalnum()):
            raise ValueError("Value must be alphanumeric string")

    def _validate_string(self, string: str):
        """
        Method for validating key
        """
        return not isinstance(string, str) or not string.isalnum()

    def set_value(self, key: str, value: str):
        """
        Method to set a value
        """
        self._validate_key_value(key, value)
        if self.db_size >= self.__MAX_DB_SIZE and self._transaction_stack:
            self.logger.error("Database is full")
            raise MemoryError("Database size limit reached")

        if self._transaction_stack:
            if self.transaction_depth >= self.__MAX_TRANSACTION_DEPTH:
                self.logger.error("Transaction depth reached")
                raise RecursionError("Maximum transaction depth reached")

            current_transaction = self._transaction_stack[-1]
            if key not in current_transaction["old_values"] and key in self._main_db:
                current_transaction["old_values"][key] = self._main_db[key]
            current_transaction["updates"][key] = value
            self.logger.info(f"SET in transaction: {key} = {value}")
        else:
            self._update_main_db(key, value)
            self.logger.info(f"SET: {key} = {value}")

    def _update_main_db(self, key: str, value: str):
        """
        Method for updating the database
        """
        if key in self._main_db:
            old_values = self._main_db[key]
            self._counts[old_values] -= 1
            if self._counts[old_values] == 0:
                del self._counts[old_values]

        self._main_db[key] = value
        self._counts[value] += 1

    def get_value(self, key: str):
        """
        Method to get value from database by key
        """
        if self._validate_string(key):
            self.logger.error(f"Invalid key. key = {key}")
            return "ERROR: Invalid key format"

        for transaction in reversed(self._transaction_stack):
            if key in transaction["updates"]:
                return transaction["updates"][key]

        self.logger.info(f"Successfully retrieved value by key: {key}")
        return self._main_db.get(key, "NULL")

    def unset_value(self, key: str):
        """
        Method to remove value with check
        """
        if self._validate_string(key):
            raise ValueError(f"Key must be alphanumeric string")

        if self._transaction_stack:
            current_transaction = self._transaction_stack[-1]
            if key not in current_transaction["old_values"] and key in self._main_db:
                current_transaction["old_values"][key] = self._main_db[key]
            current_transaction["updates"][key] = "NULL"
            self.logger.info(f"UNSET in transaction: {key}")
        else:
            if key in self._main_db:
                value = self._main_db[key]
                del self._main_db[key]
                self._counts[value] -= 1
                if self._counts[value] == 0:
                    del self._counts[value]
                self.logger.info(f"UNSET in transaction: {key}")

    def count_values(self, value: str):
        """
        Method for calculating values with verification
        """
        if self._validate_string(value):
            return "ERROR: Invalid value format"

        count = self._counts.get(value, 0)

        for transaction in self._transaction_stack:
            for key, val in transaction["updates"].items():
                if key in self._main_db or any(t['updates'].get(key, "NULL") != "NULL"
                                               for t in self._transaction_stack[:self._transaction_stack.index(transaction)]):
                    old_values = transaction["old_values"].get(key, self._main_db[key])
                    if old_values == value:
                        count -= 1
                if val == value:
                    count += 1
                elif val == "NULL" and transaction["old_values"].get(key) == value:
                    count -= 1
        return count

    def find_keys(self, value: str):
        """
        Method for finding keys with verification
        """
        if self._validate_string(value):
            return "ERROR: Invalid value format"

        keys = set()
        for key, val in self._main_db.items():
            if val == value:
                keys.add(key)

        for transaction in self._transaction_stack:
            for key, val in transaction["updates"].items():
                if val == value:
                    keys.add(key)
                elif val == "NULL" and key in keys:
                    keys.remove(key)
        return sorted(keys) if keys else "NULL"

    def begin_transaction(self):
        """
        Method to start a transaction
        """
        if len(self._transaction_stack) >= self.__MAX_TRANSACTION_DEPTH:
            raise RecursionError("Maximum transaction depth reached")
        self._transaction_stack.append({"updates": {}, "old_values": {}})
        self.logger.info("BEGIN TRANSACTION")

    def rollback_transaction(self):
        """
        Method for rolling back a transaction
        """
        if not self._transaction_stack:
            self.logger.warning("ROLLBACK attempted with no active transactions")
            return False

        self._transaction_stack.pop()
        self.logger.info("ROLLBACK TRANSACTION")
        return True

    def commit_transaction(self):
        """
        Method to commit a transaction
        """
        if not self._transaction_stack:
            self.logger.warning("ROLLBACK attempted with no active transactions")
            return False

        while self._transaction_stack:
            transaction = self._transaction_stack.pop(0)
            for key, value in transaction["updates"].items():
                if value == "NULL":
                    if key not in self._main_db:
                        old_values = self._main_db[key]
                        del self._main_db[key]
                        self._counts[old_values] -= 1
                        if self._counts[old_values] == 0:
                            del self._counts[old_values]
                else:
                    self._update_main_db(key, value)
        self.logger.info("COMMIT ALL TRANSACTION")
        return True