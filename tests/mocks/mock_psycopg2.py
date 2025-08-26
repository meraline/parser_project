"""Mock psycopg2 module for testing without database dependency"""

class MockConnection:
    def __init__(self, *args, **kwargs):
        self.closed = 0
        
    def close(self):
        self.closed = 1
        
    def cursor(self):
        return MockCursor()
        
    def commit(self):
        pass
        
    def rollback(self):
        pass

class MockCursor:
    def __init__(self):
        self.description = None
        self.rowcount = 0
        
    def execute(self, query, params=None):
        pass
        
    def fetchone(self):
        return None
        
    def fetchall(self):
        return []
        
    def fetchmany(self, size=1):
        return []
        
    def close(self):
        pass

def connect(*args, **kwargs):
    return MockConnection(*args, **kwargs)

# Mock pool module
class MockPool:
    def __init__(self, *args, **kwargs):
        pass
        
    def getconn(self):
        return MockConnection()
        
    def putconn(self, conn):
        pass
        
    def closeall(self):
        pass

class SimpleConnectionPool(MockPool):
    pass

class ThreadedConnectionPool(MockPool):
    pass

# Mock pool module
class pool:
    SimpleConnectionPool = SimpleConnectionPool
    ThreadedConnectionPool = ThreadedConnectionPool

# Mock exceptions
class Error(Exception):
    pass

class DatabaseError(Error):
    pass

class IntegrityError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass
