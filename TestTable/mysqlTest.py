import mysql.connector

class Mysql:
    def connectMysql(hostName,userName,dbPassword,dbSchema):
        try:
            mydb = mysql.connector.connect(host=hostName, user=userName, password=dbPassword, database=dbSchema)
            return mydb
        except Exception as e:
            print(e)

    def checkTableExists(dbcon, tablename):
        dbcur = dbcon.cursor()
        dbcur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(tablename.replace('\'', '\'\'')))
        if dbcur.fetchone()[0] == 1:
            dbcur.close()
            return True

        dbcur.close()
        return False

    def countRow(mydb, tableName, columnName):
        try:
            mycursor = mydb.cursor()
            sql = "select count(" + columnName + ") from" + tableName + " where " + columnName + " is not null;"#added from if error remve
            print(sql)
            mycursor.execute(sql)

            for count in mycursor:
                return int(count[0])
        except Exception as e:
            print(e)

    def countTable(mydb, tableName):
        try:
            mycursor = mydb.cursor()
            sql = "select count(*) from " + tableName + ";"
            mycursor.execute(sql)

            for count in mycursor:
                return int(count[0])
        except Exception as e:
            print(e)



#obj = Mysql
#obj.connectMysql("192.168.15.63","root","softinc", "mj_db")
"""if Mysql.checkTableExists(Mysql.connectMysql("192.168.15.63","root","softinc", "mj_db"),"t_10"):
    print("exist")
else:
    print("Not exist")"""

#print(Mysql.countTable(Mysql.connectMysql("192.168.15.63","root","softinc", "mj_db"),"t_1"))