import configparser
import logging
from collections import defaultdict
from typing import List

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
    def db_size(self) -> int:
        """
        The method returns the current size of the database
        (read-only)
        """
        return len(self._main_db)

    @property
    def transaction_depth(self) -> int:
        """
        The method returns the current transaction depth
        (read-only)
        """
        return len(self._transaction_stack)

    def _validate_key_value(self, key: str, value: str) -> None:
        """
        Method for validating input parameters
        """
        if not (isinstance(key, str) and key.isalnum()):
            raise ValueError("Key must be alphanumeric string")
        if not (isinstance(value, str) and value.isalnum()):
            raise ValueError("Value must be alphanumeric string")

    def _validate_string(self, string: str) -> bool:
        """
        Method for validating key
        """
        return not isinstance(string, str) or not string.isalnum()

    def _get_effective_db(self) -> dict[str, str]:
        effective_db = dict(self._main_db)
        for transaction in self._transaction_stack:
            for key, value in transaction["updates"].items():
                if value == "NULL":
                    effective_db.pop(key, None)
                else:
                    effective_db[key] = value

        return effective_db

    def _key_with_value(self, value: str) -> List[str] | str:
        if self._validate_string(value):
            return "ERROR: Invalid value format"

        effective_db = self._get_effective_db()
        keys = [k for k, v in effective_db.items() if v == value]
        return keys

    def set_value(self, key: str, value: str) -> None:
        """
        Method to set a value
        """
        self._validate_key_value(key, value)
        if self.db_size >= self.__MAX_DB_SIZE:
            self.logger.error("Database is full")
            raise MemoryError("Database size limit reached")

        if self._transaction_stack:
            if self.transaction_depth >= self.__MAX_TRANSACTION_DEPTH:
                self.logger.error("Transaction depth reached")
                raise RecursionError("Maximum transaction depth reached")

            current_transaction = self._transaction_stack[-1]
            if key not in current_transaction["old_values"]:
                current_transaction["old_values"][key] = self._main_db.get(key)
            current_transaction["updates"][key] = value
            self.logger.info(f"SET in transaction: {key} = {value}")
        else:
            self._update_main_db(key, value)
            self.logger.info(f"SET: {key} = {value}")

    def _update_main_db(self, key: str, value: str) -> None:
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

    def get_value(self, key: str) -> str:
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

    def unset_value(self, key: str) -> None:
        """
        Method to remove value with check
        """
        if self._validate_string(key):
            raise ValueError(f"Key must be alphanumeric string")

        if self._transaction_stack:
            current_transaction = self._transaction_stack[-1]
            if key not in current_transaction["old_values"]:
                current_transaction["old_values"][key] = self._main_db.get(key)
            current_transaction["updates"][key] = "NULL"
            self.logger.info(f"UNSET in transaction: {key}")
        else:
            if key in self._main_db:
                value = self._main_db[key]
                del self._main_db[key]
                self._counts[value] -= 1
                if self._counts[value] == 0:
                    del self._counts[value]
                self.logger.info(f"UNSET: {key}")

    def count_values(self, value: str) -> int | str:
        """
        Method for calculating values with verification
        """
        count_values = self._key_with_value(value)
        return len(count_values) if isinstance(count_values, list) else count_values

    def find_keys(self, value: str) -> List[str] | str:
        """
        Method for finding keys with verification
        """
        find_keys = self._key_with_value(value)
        return sorted(find_keys) if isinstance(find_keys, list) and find_keys else "NULL"

    def begin_transaction(self) -> None:
        """
        Method to start a transaction
        """
        if len(self._transaction_stack) >= self.__MAX_TRANSACTION_DEPTH:
            raise RecursionError("Maximum transaction depth reached")
        self._transaction_stack.append({"updates": {}, "old_values": {}})
        self.logger.info("BEGIN TRANSACTION")

    def rollback_transaction(self) -> bool:
        """
        Method for rolling back a transaction
        """
        if not self._transaction_stack:
            self.logger.warning("ROLLBACK attempted with no active transactions")
            return False

        self._transaction_stack.pop()
        self.logger.info("ROLLBACK TRANSACTION")
        return True

    def commit_transaction(self) -> bool:
        """
        Method to commit a transaction
        """
        if not self._transaction_stack:
            self.logger.warning("COMMIT attempted with no active transactions")
            return False

        transaction = self._transaction_stack.pop()
        if self._transaction_stack:
            parent_transaction = self._transaction_stack[-1]
            for key, value in transaction["updates"].items():
                if key not in parent_transaction["old_values"]:
                    parent_transaction["old_values"][key] = self._main_db.get(key)
                parent_transaction["updates"][key] = value
        else:
            for key, value in transaction["updates"].items():
                if value == "NULL" and key in self._main_db:
                    old_values = self._main_db[key]
                    del self._main_db[key]
                    self._counts[old_values] -= 1
                    if self._counts[old_values] == 0:
                        del self._counts[old_values]
                else:
                    self._update_main_db(key, value)
        self.logger.info("COMMIT TRANSACTION")
        return True