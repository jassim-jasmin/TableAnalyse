import psycopg2
import matplotlib.pyplot as plt
from TestTable import mysqlTest,psqlTest
from TestTable import Graph

class pgsqlStncCalc:
    def __init__(self):
        self.schemaName = None
        self.tableName = None
        self.codeName = None
        self.hostName = None
        self.password = None
        self.psqldb = None
        self.dataBaseName = None
        self.userName = None
        self.portNumber = None

    def assign(self,schemaName,hostName,password,dataBaseName,userName,portNumber):
        self.schemaName = schemaName
        self.hostName = hostName
        self.password = password
        self.dataBaseName = dataBaseName
        self.userName = userName
        self.portNumber = portNumber

    def assignVar(self,schemaName,tableName,codeName,hostName,password,dataBaseName,userName,portNumber):
        self.schemaName = schemaName
        self.tableName = tableName
        self.codeName = codeName
        self.hostName = hostName
        self.password = password
        self.dataBaseName = dataBaseName
        self.userName = userName
        self.portNumber = portNumber

    def connect(self):
        try:
            self.psqldb = psycopg2.connect(database=self.dataBaseName,user=self.userName,password=self.password,host=self.hostName,port=self.portNumber)
            print("connection success")
        except Exception as e:
            print("Error connecting DB")
            print(e)

    def stndCountMySQL(self,mysqldb,fieldName,tableName):
        try:
            codeCount = dict()
            mysql  = "select distinct " + fieldName + " from zillow_" + tableName + " where " + fieldName + " is not null;"
            mysqlcursor = mysqldb.cursor()
            mysqlcursor.execute(mysql)

            for codes in mysqlcursor:
                print(''.join(codes))
                nu = self.countMySQL(str(''.join(codes)), tableName, fieldName, "192.168.15.10", "root", "softinc", "nc_polk_rawdata")
                codeCount[''.join(codes)] = nu
            print(str(mysql))
            print(codeCount)
            return codeCount
        except Exception as e:
            print("print exception in mysql count")
            print(e)

    def stndCoutPG(self,fieldName):
        try:
            codeCount = dict()
            psql = "select distinct " + fieldName + " from " + self.schemaName + "." + self.tableName + " where " + fieldName + " is not null;"
            psqlcursor = self.psqldb.cursor()
            psqlcursor.execute(psql)

            for codes in psqlcursor:
                print(''.join(codes))
                num = self.countPG(''.join(codes),fieldName)
                codeCount[''.join(codes)] = num
            print(str(psql))
            return codeCount
        except Exception as e:
            print("Exception in query")
            print(e)

    def countMySQL(self, stndCode, tableName, fieldName,hostName,userName,password,schemaName):
        try:
            mysql = "select count(" + fieldName + ") from zillow_" + tableName + " where " + fieldName + " = '" + stndCode + "';"
            print(mysql)
            db = mysqlTest.Mysql.connectMysql(hostName, userName, password, schemaName)
            print(db)
            mysqlcursor = db.cursor()
            mysqlcursor.execute(mysql)
            for count in mysqlcursor:
                print(int(count[0]))
                nu = int(count[0])
                return nu
        except Exception as e:
            print("error in mysql stnd code count")
            print(e)

    def countPG(self,stndCode,fieldName):
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

    def disconnect(self):
        self.psqldb.close
        print("disconnect")

    def calculatePer(self,schemaName,tableName,codeName,hostName):
        self.assignVar(schemaName,tableName,codeName,hostName)

    def represent(self,testschema,testtable,codeName,hostName,userName,dataBaseName,password,portNumber,stndName):
        self.assignVar(testschema,testtable,codeName,hostName,password,dataBaseName,userName,portNumber)
        self.connect()
        val = []
        val = self.stndCoutPG(stndName)
        self.disconnect()
        labels = []
        size = []
        for code, num in val.items():
            labels.append("pg : " + code)
            size.append(num)

        mysqldb = mysqlTest.Mysql.connectMysql("192.168.15.10", "root", "softinc", "nc_polk_rawdata")
        valmysql = self.stndCountMySQL(mysqldb,stndName,testtable)

        for codem, numm in valmysql.items():
            labels.append("mysql : " + codem)
            size.append(numm)

        #Graph.Graph.Pi(labels,size)
        Graph.Graph.Bar(labels,size,"stnd code","count",stndName)

    def TableReport(self):
        tableName = []
        count = []
        self.assign("z_37149_20181003","192.168.15.10","softinc","zillow_2016","postgres","5432")
        self.connect()
        cursor = self.psqldb.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + self.schemaName + "'")
        for table in cursor.fetchall():
            tableName.append(''.join(table))
            count.append(psqlTest.Psql.countTable(self.psqldb, self.schemaName, ''.join(table)))
        Graph.Graph.Bar(tableName,count,"Zillow table","count", "Table report")
        #Graph.Graph.Pi(tableName,count)
        self.disconnect()

    def compareTable(self, pgHost,pgUserName,pgPassword,pgDB,pgSchemaName,mySQLHost,mySQLUserName,mySQLPassword,mySQLSchemaName):
        tableName = []
        count = []
        tableNameList = ["main","values","sales_data","name","building","building_areas","exterior_wall","extra_features","garage", "oby", "pool", "tax_exemption","care_of_name","mail_address","interior_flooring","interior_wall","tax_district","type_construction","vesting_codes"]

        for nameList in tableNameList:
            table = dict()

            pgcont = psqlTest.Psql.countTable(
                psqlTest.Psql.connectPsql(pgDB, pgUserName, pgPassword, pgHost, "5432"),
                pgSchemaName, nameList)
            mysqlcount = mysqlTest.Mysql.countTable(
                mysqlTest.Mysql.connectMysql(mySQLHost, mySQLUserName, mySQLPassword, mySQLSchemaName), "zillow_" + nameList)
            tableName.append("pg " + nameList + ": (" + str(pgcont) + ")")
            count.append(pgcont)
            tableName.append("mysql " + nameList + ": (" + str(mysqlcount) + ")")
            count.append(mysqlcount)

        #Graph.Graph.Bar(tableName, count, "Table", "count", "Compare")
        Graph.Graph.h_Bar(tableName, count, "Table", "count", "Compare")

    def compareTableCount(self, pgHost, pgUserName, pgPassword, pgDB, pgSchemaName, mySQLHost, mySQLUserName, mySQLPassword,
                     mySQLSchemaName):
        table = dict()
        tableName = []
        count = []
        tableNameList = ["main", "values", "sales_data", "name", "building", "building_areas", "exterior_wall",
                         "extra_features", "garage", "oby", "pool", "tax_exemption", "care_of_name", "mail_address",
                         "interior_flooring", "interior_wall", "tax_district", "type_construction", "vesting_codes"]

        tableData = []
        data = []

        for nameList in tableNameList:
            tableDictionary = dict()
            countDictionary = dict()

            pgcont = psqlTest.Psql.countTable(
                psqlTest.Psql.connectPsql(pgDB, pgUserName, pgPassword, pgHost, "5432"),
                pgSchemaName, nameList)
            mysqlcount = mysqlTest.Mysql.countTable(
                mysqlTest.Mysql.connectMysql(mySQLHost, mySQLUserName, mySQLPassword, mySQLSchemaName),
                "zillow_" + nameList)
            tableName.append("pg " + nameList + ": (" + str(pgcont) + ")")

            tableDictionary['Table'] = "pg " + nameList
            countDictionary['Count'] = str(pgcont)

            data.append(tableDictionary)
            data.append(countDictionary)
            #tableData.append(data)

            count.append(pgcont)
            tableName.append("mysql " + nameList + ": (" + str(mysqlcount) + ")")

            tableDictionary['Table'] = "mysql " + nameList
            countDictionary['Count'] = str(mysqlcount)

            data.append(tableDictionary)
            data.append(countDictionary)

            count.append(mysqlcount)

            table["pg " + nameList + ": (" + str(pgcont) + ")"] = pgcont
            table["mysql " + nameList + ": (" + str(mysqlcount) + ")"] = mysqlcount

        tableData.append(data)
        # Graph.Graph.Bar(tableName, count, "Table", "count", "Compare")
        Graph.Graph.h_Bar(tableName, count, "Table", "count", "Compare")
        #return tableData
        #return table
        return data

    def compareTableCountForAngular(self, pgHost, pgUserName, pgPassword, pgDB, pgSchemaName, mySQLHost, mySQLUserName,
                          mySQLPassword,
                          mySQLSchemaName, nameList):

        #tableNameList = ["main", "values", "sales_data", "name", "building", "building_areas", "exterior_wall","extra_features", "garage", "oby", "pool", "tax_exemption", "care_of_name", "mail_address","interior_flooring", "interior_wall", "tax_district", "type_construction", "vesting_codes"]

        tableData = []
        data = []

        tableDictionary = dict()
        countDictionary = dict()
        tableDictionaryForPg = dict()
        countDictionaryForPg = dict()

        pgcont = psqlTest.Psql.countTable(
            psqlTest.Psql.connectPsql(pgDB, pgUserName, pgPassword, pgHost, "5432"),
            pgSchemaName, nameList)
        mysqlcount = mysqlTest.Mysql.countTable(
            mysqlTest.Mysql.connectMysql(mySQLHost, mySQLUserName, mySQLPassword, mySQLSchemaName),
            "zillow_" + nameList)

        tableDictionaryForPg['Table'] = "pg"
        tableDictionaryForPg['Count'] = pgcont

        data.append(tableDictionaryForPg)

        tableDictionary['Table'] = "mysql"
        tableDictionary['Count'] = mysqlcount

        data.append(tableDictionary)

        return data

    def compareTableChart(self, pgHost, pgUserName, pgPassword, pgDB, pgSchemaName, mySQLHost, mySQLUserName,
                          mySQLPassword,
                          mySQLSchemaName):

        tableNameList = ["main", "values", "sales_data", "name", "building", "building_areas", "exterior_wall",
                         "extra_features", "garage", "oby", "pool", "tax_exemption", "care_of_name", "mail_address",
                         "interior_flooring", "interior_wall", "tax_district", "type_construction", "vesting_codes"]

        fieldName = []
        count = []
        data = []

        for nameList in tableNameList:

            pgcont = psqlTest.Psql.countTable(
                psqlTest.Psql.connectPsql(pgDB, pgUserName, pgPassword, pgHost, "5432"),
                pgSchemaName, nameList)
            mysqlcount = mysqlTest.Mysql.countTable(
                mysqlTest.Mysql.connectMysql(mySQLHost, mySQLUserName, mySQLPassword, mySQLSchemaName),
                "zillow_" + nameList)

            fieldName.append("pg " + nameList)
            count.append(str(pgcont))

            fieldName.append("mysql " + nameList)
            count.append(str(mysqlcount))

        chartData = dict()

        chartData['FieldName'] = fieldName
        chartData['Count'] = count
        print(chartData)
        return chartData

#obj = pgsqlStncCalc()

#obj.compareTable("192.168.15.10","postgres","softinc","zillow_2016","z_37149_20181003","192.168.15.10","root","softinc","nc_polk_rawdata")
#obj.TableReport()
#obj.represent("z_37149_20181003","garage","garage_stnd_code","192.168.15.10","postgres","zillow_2016","softinc","5432","garage_stnd_code")

#print(obj.stndCountMySQL(mysqlTest.Mysql.connectMysql("192.168.15.10", "root", "softinc", "nc_polk_rawdata"),"extra_features_stnd_code","extra_features"))
