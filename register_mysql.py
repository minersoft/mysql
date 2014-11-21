# 
# Copyright Michael Groys, 2014
#

import m.db
import m.db.sqlite_engine
import mysql_db_engine
db_engine = mysql_db_engine.MySqlDbEngine()
m.db.registerEngine("mysql", db_engine)
