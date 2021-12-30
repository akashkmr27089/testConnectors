import os 
import snowflake.connector
'''
Snowflake Service for Extracting Tracking ids
Code author aakash_kumar@maplemonk.com
'''

class SnowflakeService: 

    def __init__(self, user, password, account, warehouse, database, role) -> None:
        self.connectionObject = None
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.role = role
        print(self.user, self.account, self.warehouse, self.database, self.role)

    def connect(self):
        try: 
            ctx = snowflake.connector.connect(
                    user=self.user,
                    password=self.password,
                    account=self.account,
                    warehouse=self.warehouse,
                    database=self.database,
                    role=self.role
                )
            print("&&"*30, ctx)
            self.connectionObject = ctx
        except Exception as ex : 
            return [True, f"Error While connecting to the Snowflake ", ex]

    def returnConnectionObject(self):
        return self.connectionObject

    def iter_row(self, cursor, size=10):
        while True:
            rows = cursor.fetchmany(size)
            if not rows:
                break
            for row in rows:
                yield row

    def executeQuerry(self, query):
        finalObject = []
        try: 
            if(self.connectionObject == None): 
                pass
            else: 
                cs = self.connectionObject.cursor()
                try:
                    cs.execute(query)
                    one_row = cs.fetchall()
                    finalObject =  one_row
                finally:
                    cs.close()
        except Exception as ex: 
            print("Some Error Occured during Snowflake Exectution :", ex)
        return finalObject

    def executeQueryStream(self, query):
        finalObject = []
        print("Inside Streaming ")
        try: 
            if(self.connectionObject == None): 
                pass
            else: 
                cs = self.connectionObject.cursor()
                try:
                    cs.execute(query)
                    for row in self.iter_row(cs, 10):
                        yield row
                except Exception as ex:
                    print("Exception Occured During ")
                    cs.close()
        except Exception as ex: 
            print("Some Error Occured during Snowflake Exectution :", ex)
        yield finalObject

    def closeConnection(self):
        if(self.connectionObject != None):
            self.connectionObject.close()

    ## Util Functions 
    def createTableString(self, tableDict):
        finalString = ''
        for doc in tableDict:
            if tableDict[doc] == 'string':
                tableDict[doc] = 'varchar(200)'
            finalString += f" {doc} {tableDict[doc]},"
            # finalString += f" {doc} variant,"
        if(finalString != ''):
            finalString = finalString[:-1]
        return finalString

    def restructuredJson(self, data):
        # This function will take Dictionary as an input and returns a 
        # parsing supported json files 
        if(isinstance(data, type({}))) :
            for doc in data:
                if(not isinstance(data[doc], type(""))):
                    data[doc] = str(data[doc])
                    data[doc].replace(' None', " 'None'")
        return data

    def restructuredData(self, data):
        data.replace(' None', " 'None'")
        return data

    def deBatchify2(self, batchData, specFile):
        batchSize = len(batchData)
        # Getting Spec File List 
        listA = '('
        arrayList = []
        for data in specFile:
            listA += f" {data},"
            arrayList.append(data)
        listA = listA[:-1]
        listA += ")"

        # Extract Object List 
        finalObjList = []
        for data in batchData:
            tempScript = ''
            for keys in arrayList:
                if(specFile[keys] == 'object' or specFile[keys] == 'variant' or specFile[keys] == 'array'):  
                    # restructuredJson = self.restructuredJson(data[keys])
                    tempScript += f"parse_json($${data[keys]}$$),"
                elif(specFile[keys] != 'boolean'): 
                    tempScript += f"'{data[keys]}',"
                else: 
                    tempScript += f"{data[keys]},"
                # tempScript += f"parse_json($${data[keys]}$$),"
            if(tempScript != ""):
                tempScript = tempScript[:-1]
            tempScript = tempScript.replace(' None', " 'None'")
            finalObjList.append(tempScript)
        return [listA,finalObjList]


    def deBatchify(self, batchData, specFile):
        batchSize = len(batchData)
        # Getting Spec File List 
        listA = '('
        arrayList = []
        for data in specFile:
            listA += f" {data},"
            arrayList.append(data)
        listA = listA[:-1]
        listA += ")"

        # Extract Object List 
        finalObjectString = ''
        objectString = '('
        if(batchSize > 0):
            objectString = '('
            for data in batchData:
                for keys in arrayList:
                    objectString += f" {data[keys]},"
                objectString = objectString[:-1]
                objectString += ")"
                # print(objectString)
                finalObjectString += f" {objectString},"
        finalObjectString = finalObjectString[:-1]

        return [listA, finalObjectString]
        

