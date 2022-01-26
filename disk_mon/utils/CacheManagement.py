import sqlite3

class CacheManagement:

    def __init__(self, sqlite_file_path):

        self.cache_conn = sqlite3.connect(sqlite_file_path)
        self.cache_conn.row_factory = sqlite3.Row
    
    def cache_execute(self, query):
        cursor = None
        # TODO : performance testing and if required closing the cursor. 
        with self.cache_conn:
            cursor = self.cache_conn.execute(query)
        return cursor
    
    def commit(self):
        self.cache_conn.commit()

    def roll_back(self):
        self.cache_conn.rollback()
