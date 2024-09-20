import pyodbc as pdb
import pandas as pd
import datetime


def openConn(secrets, conn_str=None):
    if conn_str is None:        
        driver= '{ODBC Driver 17 for SQL Server}' 
        server = 'tcp:' + secrets.get('server') 
        database = secrets.get('database') 
        username = secrets.get('username') 
        password = secrets.get('password')      
        conn_str= 'DRIVER=' + driver + ';SERVER=' +server + ';PORT=1433;DATABASE=' + database +';UID=' + username + ';PWD=' + password        
    conn = pdb.connect(conn_str)
    return conn

# Option to limit results returned
# If result total is less than result total required by top then values between start index and end index are returned.
# Top set to zero (default) brings all results back unless a starting point is set in which case start to end is returned.
def getTable(secrets, qry=None, tbl = None, top = 0, start=0):
    
    conn = openConn(secrets)
    cursor = conn.cursor()
    
    if qry is None:
        if tbl is None:
            return None
        qry = f'Select * from {tbl}'

    cursor.execute(qry)
    results =  cursor.fetchall() # Returns empty list if no results
    if start > len(results):
        return [] # Returns empty list
    if top == 0 or start + top > len(results):
        return results[start-1:len(results)]
    else:
        return results[start-1:top + start - 1]
    
def getFieldFromTable(secrets, field, tbl, top=0, start=0):
    qry = f'Select {field} from {tbl}'
    return getTable(secrets, qry, top=top, start=start)

def loadRecord(secrets, tblNm, cols, rec):
    conn = openConn(secrets)
    #conn = openConn('FRCLab1')
    cursor = conn.cursor()
    #values = ','.join(f"'{r}'" for r in rec)
    #sql = f"INSERT INTO {tblNm} ({','.join(cols)}) VALUES ({values})"
    # You can just write out a sql query as we do below but by injecting values at point of execution, pyodb handles the conversion of native data types to db equivalents.
    valuePlaceHolders =  "?" + ",?" * (len(cols) - 1)
    colNames = ",".join(cols)    
    # This is a hack. Usual way to convert to correct db types in to let the pyodbc cursor take the strain via injection as per commented out lines below
    # Except if you do this SELECT SCOPE_IDENTITY() doesn't return a result. Tried all kinds of variations to get this to return id of last record added to no avail
    # We do use injection in loadTbl where we don't need to return a result.
    #sqlInsert = f"INSERT INTO {tblNm} ({colNames}) VALUES ({valuePlaceHolders})"
    #cursor.execute(sqlInsert, rec)
    values = ','.join(f"'{r}'" for r in rec)
    values = values.replace("'None'","NULL")
    sqlInsert =   f"INSERT INTO {tblNm} ({colNames}) VALUES ({values})"
    cursor.execute(sqlInsert)
    conn.commit() # Necessary for db changes eg. inserts but not select statements. or could conn.autocommit = True so commit happens automatically - untested.
    sql = 'SELECT SCOPE_IDENTITY()'
    cursor.execute(sql)
    id = next(cursor, [None])[0] #Way of getting single record result ([None] stops is failing if nothing returned)
    cursor.close()
    return id

def loadTbl(secrets, tblNm, cols, data):  
    
    conn = openConn(secrets)
    cursor = conn.cursor()
    
    valuePlaceHolders =  "?" + ",?" * (len(cols) - 1)
    colNames = ",".join(cols)    
    sqlInsert = f"INSERT INTO {tblNm} ({colNames}) VALUES ({valuePlaceHolders})"
    cursor.executemany(sqlInsert, data)

    conn.commit()
    cursor.close()

    return True


    
