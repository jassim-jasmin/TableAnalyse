import psycopg2

class Psql:
    def connectPsql(databaseName, userName, passwordData, hostAddress, portNumber):
        try:
            psqldb = psycopg2.connect(database=databaseName, user=userName, password=passwordData, host=hostAddress,
                                  port=portNumber)
            return psqldb
        except Exception as e:
            print(e)

    def checkTableExist(db,tableName):
        try:
            cur = db.cursor()
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName,))
            return cur.fetchone()[0]
        except Exception as e:
            print(e)

    def count(self,stndCode,fieldName):
        try:
            psql = "select count(" + fieldName + ") from " + self.schemaName + "." + self.tableName + " where " +  fieldName + " = " + " '" + stndCode + "';"
            print(psql)
            psqlcursor = self.psqldb.cursor()
            psqlcursor.execute(psql)
            for count in psqlcursor:
                print(int(count[0]))
                num = int(count[0])
                return num
        except Exception as e:
            print("Error in execution of query")
            print(e)

    def countTable(mydb, schemaName, tableName):
        try:
            mycursor = mydb.cursor()
            sql = "select count(*) from " + schemaName + "." + tableName + ";"
            mycursor.execute(sql)

            for count in mycursor:
                return int(count[0])
        except Exception as e:
            print(e)

"""if Psql.checkTableExist(Psql.connectPsql("testdb","postgres","postgres","127.0.0.1","5432"),"testtable1"):
    print("exist")
else:
    print("No")"""