# 
# Copyright Michael Groys, 2014
#

import m.db.std_engine as std_engine 
from miner_globals import setGlobalCompletionState, resetGlobalCompletionState
import m.common as common

class MySqlDbCursor(std_engine.FetchCursor):
    def __init__(self, dbcursor):
        std_engine.FetchCursor.__init__(self, dbcursor)
    def close(self):
        # mysql connertors requires that all reults are fetched
        if self.dbcursor:
            self.dbcursor.fetchall()
        std_engine.FetchCursor.close(self)

class MySqlDbConnection(std_engine.Connection):
    def __init__(self, dbconnection, executemanyBatchSize, engine):
        std_engine.Connection.__init__(self, dbconnection, engine)
        self.executemanyBatchSize = executemanyBatchSize
    def createFetchCursor(self, dbcursor, numRowsToFetchAtOnce=1):
        '''
        This function creates fetch cursor based on database cursor.
        It receives number of rows to fetch at once (this is recommendation value)
        If it is 0 - fetch all rows at once
        '''
        return MySqlDbCursor(dbcursor)
    def executemany(self, query, seq_of_params):
        std_engine.Connection.executemany_in_batches(self, self.executemanyBatchSize, query, seq_of_params)

class MySqlDbEngine(std_engine.Engine):
    def __init__(self):
        std_engine.Engine.__init__(self)
    def connect(self, dbtype, parsedUrl, **kwargs):
        import mysql.connector
        import getpass
        username = parsedUrl.username
        password = parsedUrl.password
        if not username:
            setGlobalCompletionState(common.COMPLETE_NONE)
            username = raw_input("Login name: ")
            while len(username) == 0:
                username = raw_input("? ")
            resetGlobalCompletionState()
        if not password:
            password = getpass.getpass("Password: ")
        if len(parsedUrl.path)>1 and parsedUrl.path.startswith("/"):
            database = parsedUrl.path[1:]
        else:
            setGlobalCompletionState(common.COMPLETE_NONE)
            database = raw_input("Database: ")
            while len(database) == 0:
                database = raw_input("? ")
            resetGlobalCompletionState()
        if parsedUrl.port:
            port = parsedUrl.port
        else:
            port = 3306
        try:
            cnx = mysql.connector.connect(user=username, password=password, database=database,
                                          host=parsedUrl.hostname, port=port)
        except Exception as e:
            raise common.MiningError("Failed to connect to database: %s" % str(e))
        try:
            executemanyBatchSize = int(kwargs.get("batch_size", 100))
        except:
            raise common.CompilationError("batch_size parameter should be integer")
        return MySqlDbConnection(cnx, executemanyBatchSize, self)
    
    def getTableNamesQuery(self):
        return "SHOW TABLES"
