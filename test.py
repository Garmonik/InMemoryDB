import unittest
from database import InMemoryDB

class TestInMemoryDB(unittest.TestCase):
    def setUp(self):
        self.db = InMemoryDB()

    def test_transaction_commit_rollback(self):
        self.db.set_value('a', '1')
        self.db.begin_transaction()
        self.db.set_value('a', '2')
        self.db.begin_transaction()
        self.db.set_value('a', '3')

        commit_result = self.db.commit_transaction()
        self.assertTrue(commit_result)

        value = self.db.get_value('a')
        self.assertEqual(value, '3')

        rollback_result = self.db.rollback_transaction()
        self.assertTrue(rollback_result)

        value_after_rollback = self.db.get_value('a')
        self.assertEqual(value_after_rollback, '1')

    def test_count_values(self):
        self.db.set_value('a', '1')
        self.db.set_value('b', '1')
        self.db.set_value('c', '2')
        self.assertEqual(self.db.count_values('1'), 2)
        self.assertEqual(self.db.count_values('2'), 1)
        self.assertEqual(self.db.count_values('3'), 0)

    def test_find_keys(self):
        self.db.set_value('a', '1')
        self.db.set_value('b', '1')
        self.db.set_value('c', '2')
        self.assertEqual(self.db.find_keys('1'), ['a', 'b'])
        self.assertEqual(self.db.find_keys('2'), ['c'])
        self.assertEqual(self.db.find_keys('3'), 'NULL')



if __name__ == '__main__':
    unittest.main()
