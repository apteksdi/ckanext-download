import ckan.plugins as plugins
import ckan.lib.helpers as helpers
import ckan.plugins.toolkit as toolkit
import ckan.lib.base as base
import ckan.lib.render as render
import routes.mapper
from routes import redirect_to
import os.path
#import py2psql
import json
from ckan.plugins.toolkit import url_for, redirect_to, request, config
import re
from helpers import * 
#import peroid

import psycopg2

class py2psql:
    
    # private member
    # host : URL or IP
    # port : postgresql server port
    # db : as a string
    # tb : as a string
    # user : as a string
    # pwd : as a string
    # data : as a dictionary, { colName : colValue }
    # columns : save table schema
    # datatype : save column data tpye { "col" : { "type" : "code", "null" : "True/False" } }
    # retStatus : returned data as a dictionary
    __host = ""
    __port = ""
    __db = ""
    __tb = ""
    __user = ""
    __pwd = ""
    __columns = []
    __datatype = {}
    __retStatus = { }

    #
    # desc : constructor
    # param@getTB : can be null when only using execsql()
    #
    def __init__(self, getHost, getPort, getDB, getTB, getUser, getPwd):
        self.__host = getHost
        self.__port = getPort
        self.__db = getDB
        self.__tb = getTB
        self.__user = getUser
        self.__pwd = getPwd
        self.__columns = []
        self.__datatype = {}
        self.__retStatus = { "state" : 0, "data" : {}, "info" : "" }

        # fetch column information
        if len(getTB) > 0:
            self.__tableSchema()

    # ------------------------
    # private member
    # ------------------------            
    #
    # desc : define server DSN
    # retn : string
    #
    def __serverDSN(self):
        conStr = ["host=" + self.__host, "port=" + self.__port, "dbname=" + self.__db, "user=" + self.__user , "password=" + self.__pwd]
        return ' '.join(conStr)

    #
    # desc : get table schema
    # retn : None
    #
    def __tableSchema(self):
        # Connect to an existing database
        conn = psycopg2.connect(self.__serverDSN())
        
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # select sql
        selectStr = "select * from " + self.__tb

        cur.execute(selectStr)
        
        # get columns
        self.__columns = [desc[0] for desc in cur.description]

        # close communication
        cur.close()
        conn.close()

    #
    # desc : get table colunm data type
    # retn : column data type in the table
    #
    def __tableColDatatype(self):
        # Connect to an existing database
        conn = psycopg2.connect(self.__serverDSN())
        
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # select sql
        selectStr = "select * from " + self.__tb

        cur.execute(selectStr)
        
        # get column data type
        for item in cur.description:
            self.__datatype.setdefault(item[0], { "type" : item[1] , "null" : item[6] })
        
        # close communication
        cur.close()
        conn.close()

        return self.__datatype
        
        
    #
    # desc : get col index in the column order
    # retn : -1 (None) or Number
    #
    def __getColIndex(self, getColName):
        if getColName in self.__columns:
            return self.__columns.index(getColName)
        else:
            return -1

    #
    # desc : set returned status
    # retn : None
    #
    def __setStatus(self, getStatus, getInfo, getData):
        self.__retStatus["state"] = getStatus
        self.__retStatus["data"] = getData
        self.__retStatus["info"] = getInfo

    #
    # desc : get column description on the execution pointer
    # param@getCur : a psycopg2 connect cursor
    # param@curIndex : index on the cursor description
    # retn : [] data type
    #
    def __getCurDesc(self, getCur, curIndex):
        curInfo = [desc[curIndex] for desc in getCur.description]
        return curInfo

    # ------------------------
    # public member
    # ------------------------  
                
    #
    # desc : returned status
    # retn : return executing status
    #
    def status(self):
        return self.__retStatus
    
    #
    # desc : get table schema
    # param@getTable : get desired table schema
    # param@descIndex : description index of table schema, -1 : means all
    # retn : status object
    #
    def getTableSchema(self, getTable=None, descIndex=0):    
        if self.__tb == "" and getTable == None:
            self.__setStatus("failure","There is no table assigned.",{})
        elif self.__tb != "" and getTable == None:
            try:
                self.__tableSchema()
                self.__setStatus("success","Get the table schema.", self.__columns)
            except:
                self.__setStatus("failure","Can not get the table schema.", self.__columns)
        elif getTable != None:
            # Connect to an existing database
            conn = psycopg2.connect(self.__serverDSN())
        
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # select sql
            selectStr = "select * from " + getTable

            try:
                cur.execute(selectStr)
        
                # get columns desc
                if descIndex < 0:
                    getColDesc = [desc for desc in cur.description]
                else:
                    getColDesc = [desc[descIndex] for desc in cur.description]
                self.__setStatus("success","Get the table schema.", getColDesc)
            except:
                self.__setStatus("failure","Can not get the table schema.", {})

            # close communication
            cur.close()
            conn.close()             
            
        return self.__retStatus
                
    #
    # desc : select operation
    # param@getConds : {}, defined where SQL conditions
    # param@getParams : [], selected column names, empty : means all
    # param@asdict : boolean, returned row as dictionary data type
    # retn : data as [] type
    # note : also support status object, use status()
    #
    def select(self, getConds, getParams, asdict=False):
        # filter the column value
        colSelected = "*"
        colList = []
        retdata = []
        dataTuple = ()
        
        # check column existing
        if len(getParams) > 0:
            for item in getParams:        
                if self.__getColIndex(item) > -1:
                    colList.append(item)
        
        # set selected columns
        if len(colList) > 0:
            colSelected = ','.join(colList)

        try:
                                
            # Connect to an existing database
            conn = psycopg2.connect(self.__serverDSN())
            
            # Open a cursor to perform database operations
            cur = conn.cursor()
    
            # select sql
            selectStr = "select " + colSelected + " from " + self.__tb
    
            if len(getConds.keys()) > 0:
                selectStr += " where "
                item = 0
                for key, value in getConds.iteritems():
                    if item != 0:
                        selectStr += " and "
                    selectStr += str(key) + "= %s "
                    item += 1
                    dataTuple += (value,)
            selectStr += ";"                                                
        
            # parameter-based select sql
            cur.execute(selectStr, dataTuple)
    
            # get all data    
            rawdata = cur.fetchall()
            
            # modify data to customized type
            if asdict:
                if len(colList) > 0:
                    for pair in rawdata:
                        tmpDict = {}
                        for item in range(0,len(pair),1):
                            tmpDict.setdefault(colList[item],pair[item])
                        retdata.append(tmpDict)
                else:
                    for pair in rawdata:
                        tmpDict = {}
                        for item in range(0,len(pair),1):
                            tmpDict.setdefault(self.__columns[item],pair[item])
                        retdata.append(tmpDict)
            else:
                retdata = rawdata
                
            # close communication
            cur.close()
            conn.close()          
        
            # set status
            self.__setStatus("success", "Select operation succeeded.", retdata)

        except:
            self.__setStatus("failure", "Select operation executed failed.", retdata)
                                               

        return retdata

    #
    # desc : update operation
    # param@getParams : {}, set sql parameters    
    # param@getConds : {}, where sql conditions
    # retn : 0 : failure, 1 : success
    # note : also support status object, use status()
    #
    def update(self, getParams, getConds):
        # 0 : failure, 1 : success
        retStatus = 1
    
        # filter the column value
        paraKeys = getParams.keys()
        condKeys = getConds.keys()
        paraList = []
        condList = []
        dataTuple = ()
        
        # check parameter existing
        if len(paraKeys) > 0:
            for item in paraKeys:     
                if self.__getColIndex(item) > -1:
                    paraList.append(item)
        else:
            retStatus = 0
            self.__setStatus("failure","Set SQL was checked in failure.",{})
            return retStatus
        
        # check condition existing
        if len(condKeys) > 0:
            for item in condKeys:     
                if self.__getColIndex(item) > -1:
                    condList.append(item)
        else:
            retStatus = 0
            self.__setStatus("failure","Where SQL was checked in failure.",{})
            return retStatus

        # update sql
        updateStr = "update " + self.__tb + " set "
        
        if len(paraList) > 0:
            paraListItem = []
            for item in paraList:
                paraListItem.append(item + "= %s ")
                dataTuple += (getParams[item],)
            updateStr += ' , '.join(paraListItem)
        else:
            retStatus = 0
            self.__setStatus("failure","Set SQL was checked in failure.",{})
            return retStatus
            
        updateStr += " where "

        if len(condList) > 0:
            condListItem = []
            for item in condList:
                condListItem.append(item + "= %s ")
                dataTuple += (getConds[item],)
            updateStr += ' and '.join(condListItem)
        else:
            retStatus = 0
            self.__setStatus("failure","Where SQL was checked in failure.",{})
            return retStatus
        
        updateStr += ";"                     

        try:
            # Connect to an existing database
            conn = psycopg2.connect(self.__serverDSN())
            
            # Open a cursor to perform database operations
            cur = conn.cursor()     
    
            # parameter-based update sql
            cur.execute(updateStr, dataTuple)
    
            # get all data    
            conn.commit()
    
            # close communication
            cur.close()
            conn.close()       
            
            self.__setStatus("success","Update operation succeeded.",{})
            
        except:      
                                
            retStatus = 0
            self.__setStatus("failure","Update operation was executed in failure.",{})

        return retStatus        
        
    #
    # desc : insert operation
    # param@getParams : {}, value sql parameters    
    # retn : 0 : failure, 1 : success
    # note : also support status object, use status()
    #
    def insert(self, getParams):
        # 0 : failure, 1 : success
        retStatus = 1
    
        # filter the column value
        paraKeys = getParams.keys()
        paraList = []
        insertedData = ()
        
        # check parameter existing
        if len(paraKeys) > 0:
            for item in paraKeys:     
                if self.__getColIndex(item) > -1:
                    paraList.append(item)
        else:
            retStatus = 0
            self.__setStatus("failure","Data parameter was empty.",{})
            return retStatus
            
        # insert string
        insertStr = "insert into " + self.__tb + " ("
        
        for index in range(0,len(paraList),1):
            if index != 0:
                insertStr += ', '
            insertStr += paraList[index]
        
        insertStr += ') values ('
        
        for index in range(0,len(paraList),1):
            if index != 0:
                insertStr += ', '
            insertStr += "%s"
            insertedData += (getParams[paraList[index]],)
        
        insertStr += ')'
        
        try:
        
            # Connect to an existing database
            conn = psycopg2.connect(self.__serverDSN())
            
            # Open a cursor to perform database operations
            cur = conn.cursor()     

            # parameter-based insertion sql
            cur.execute(insertStr,insertedData)

            # get all data    
            conn.commit()

            # close communication
            cur.close()
            conn.close()
            
            self.__setStatus("success","Insert operation succeeded.",{})
        
        except:
            self.__setStatus("failure","Insert operation was executed in failure.",{})
            retStatus = 0
        
        return retStatus

    #
    # desc : delete operation
    # param@getConds : {}, where sql conditions
    # retn : 0 : failure, 1 : success
    # note : also support status object, use status()
    #
    def delete(self, getConds):
        # 0 : failure, 1 : success
        retStatus = 1
    
        # filter the column value
        condKeys = getConds.keys()
        condList = []
        selectedTuple = ()
        
        # check parameter existing
        if len(condKeys) > 0:
            for item in condKeys:     
                if self.__getColIndex(item) > -1:
                    condList.append(item)
        else:
            retStatus = 0
            self.__setStatus("failure","Where parameter was empty.",{})
            return retStatus

        # no where condition
        if len(condList) < 1:
            retStatus = 0
            self.__setStatus("failure","Value in where parameter was empty.",{})
            return retStatus
            
        # delete string
        deleteStr = "delete from " + self.__tb + " where "
        
        for index in range(0,len(condList),1):
            if index != 0:
                deleteStr += ' and '
            deleteStr += condList[index] + " = " + "%s"
            selectedTuple += (getConds[condList[index]],)
            
        # delete transaction
        try:
        
            # Connect to an existing database
            conn = psycopg2.connect(self.__serverDSN())
            
            # Open a cursor to perform database operations
            cur = conn.cursor()     

            # parameter-based sql
            cur.execute(deleteStr, selectedTuple)

            # get all data    
            conn.commit()

            # close communication
            cur.close()
            conn.close()
            
            self.__setStatus("success","Delete operation succeeded.",{})
        
        except:
            self.__setStatus("failure","Delete operation was executed in failure.",{})
            retStatus = 0
        
        return retStatus

    #
    # desc : execute complex sql command
    # param@getSQL : parameter-based complex sql command
    # e.g. "select * from public.user where name = %(name)s;"
    # param@hasRetValue : are there returned values ?
    # param@getParams : {}
    # e.g. {'name' : "test114"}
    # param@asdict : only works when param@hasRetValue is true, returned value as dictionary data type
    # retn : return executing status
    #
    def execsql(self, getSQL, hasRetValue, getParams, asdict=True):
        # save returned data as dictionary data type
        retData = []
        
        # check data type is allowed
        if not isinstance(getParams, dict):
            self.__setStatus("failure", "Parameters must be as dictionary type.", {})
            return
        
        try:
            # connect to db       
            conn = psycopg2.connect(self.__serverDSN())
        except:
            self.__setStatus("failure", "Can not connect to db.", {})
            return

        try:
            # Open a cursor to perform database operations
            cur = conn.cursor()
        
            # parameter-based select sql
            cur.execute(getSQL, getParams)
        except:
            self.__setStatus("failure", "SQL was executed in failure.", {})
            return
        
        rawdata = {}
        try:
            if hasRetValue:
                # select
                
                # get columns
                execColumns = self.__getCurDesc(cur, 0)
                
                # get all data	
                rawdata = cur.fetchall()
                
                if asdict:
                    # set transform tuple data type into dictionary data type
                    tmp = {}
                    for item in range(0, len(rawdata), 1):
                        tmp = {}
                        for col in range(0, len(execColumns), 1):
                            tmp.setdefault(execColumns[col], rawdata[item][col])
                        retData.append(tmp)
            else:
                # insert, delete, update
                conn.commit()
            
            # close communication
            cur.close()
            conn.close()

        except:
            self.__setStatus("failure", "Data can not be queried or SQL command can not be executed.", {})
            return

        if hasRetValue:
            if asdict:
                self.__setStatus("success", "SQL command was executed.", retData)
            else:
                self.__setStatus("success", "SQL command was executed.", rawdata)
        else:
            self.__setStatus("success", "SQL command was executed.", {})
        return	

    #
    # desc : create table based on schema
    # param@tableName : name of the table for creation
    # param@tableSchema : { 'colName' : 'colSchema', '' : '' }
    # param@dropFirst : whether to drop table first if it exists
    # retn : None, call status() to get status object
    #
    def createTable(self, tableName, tableSchema, dropFirst=False):
        
        if not (isinstance(tableSchema, dict)):
            self.__setStatus("failure", "Parameters are not correct.", {})
            return
            
        # check table status (whether it exists or not)
        try:
            self.execsql("select * from information_schema.tables where table_name = %(name)s;", True, {'name' : tableName})
        except:
            self.__setStatus("failure", "Can not get the table list.", {})
            return
            
        if self.__retStatus["state"] != "success":
            self.__setStatus("failure", "Can not check table status.", {})
            return
        
        if len(self.__retStatus["data"]) > 0:
            # table already exists
            if dropFirst:
                # delete first
                self.execsql("drop table if exists " + tableName + ";", False, {})
                
                if self.__retStatus["state"] != "success":
                    self.__setStatus("failure", self.__setStatus["data"] + " Can not drop the data table.", {})
                    
            else:
                self.__setStatus("failure", "The table already exists, if it does not drop, the table can not be created.", {})
                return

        # create table
        tmpKey = tableSchema.keys()
        createTBCmd = "create table if not exists " + tableName + " ( "
        for colIndex in range(0, len(tmpKey), 1):
            if colIndex != 0:
                createTBCmd += ', '
            createTBCmd += tmpKey[colIndex] + " " + tableSchema[tmpKey[colIndex]]
        createTBCmd += " );"
            
        try:
            self.execsql(createTBCmd, False, {})
        except:
            self.__setStatus("failure", "Unexcepted error on creating the data table.", {})
            return
        
        if self.__retStatus["state"] != "success":
            self.__setStatus("failure", "Can not create data table.", {})
            return
        else:
            self.__setStatus("success", "Create data table successfully.", {})

    #
    # desc : alter table schema
    # param@tableName : table for altering
    # param@tableSchema : { 'colName' : 'new col schema' }
    # param@createTableFirstIfNotExisted : whether to create table first if table does not exist
    # param@addColIfNotExisted : whether to add column if it does not exist
    # param@theSameWithThisSchema : whether to fit the table with the input schema
    # retn : None, call status() to get status object
    # note : if addColIfNotExisted == False, the column for altering would be skipped 
    #
    def alterTable(self, \
                   tableName, \
                   tableSchema, \
                   createTableFirstIfNotExisted=True, \
                   addColIfNotExisted=True,\
                   theSameWithThisSchema=True):
        
        if not (\
                isinstance(tableName, str) and \
                isinstance(tableSchema, dict) and\
                isinstance(createTableFirstIfNotExisted, bool) and\
                isinstance(addColIfNotExisted, bool)
               ):
            self.__setStatus("failure", "Parameters are not correct.", {})
            return    
    
        # check table status (whether it exists or not)
        try:
            self.execsql("select * from information_schema.tables where table_name = %(name)s;", True, {'name' : tableName})
        except:
            self.__setStatus("failure", "Can not get the table list.", {})
            return
            
        if self.__retStatus["state"] != "success":
            self.__setStatus("failure", "Can not check table status.", {})
            return
        
        # table does not exist
        if len(self.__retStatus["data"]) < 1:
            if createTableFirstIfNotExisted:
                # create table first
                self.createTable(tableName, tableSchema, False)
                
                if self.__retStatus["state"] != "success":
                    self.__setStatus("failure", self.__retStatus["info"] + " Can not create the data table.", {})
                    return
            else:
                self.__setStatus("failure", "The table does not exist, if it does not be created, the alter operation would be stop.", {})
                return
        # table exists
        else:
            # get table column name
            crtColName = self.getTableSchema(tableName, 0)['data']
            
            warningFlag = 0
            warningMsg = ""
            for name, schema in tableSchema.iteritems():
                if name in crtColName:
                    # the same column name
                    self.execsql(\
                        "alter table " + tableName + " alter column " + name + " type " + schema + " ;",
                        False,
                        {},
                        False
                    )
                    
                    if self.__retStatus["state"] != "success":
                        warningFlag = 1
                        warningMsg = warningMsg + ' [alter column failure]' + self.__retStatus["info"]

                    # remove the column from the list
                    # the left column in the list may be dropped
                    crtColName.remove(name)
                else:
                    # there is no existing column
                    if addColIfNotExisted:
                        self.execsql(\
                            "alter table " + tableName + " add column " + name + " " + schema + " ;",
                            False,
                            {},
                            False
                        )
                        
                        if self.__retStatus["state"] != "success":
                            warningFlag = 1
                            warningMsg = warningMsg + ' [add column failure]' + self.__retStatus["info"]
                    else:
                        warningFlag = 1
                        warningMsg = warningMsg + ' [not to add column] The column ' + name + ' does not exist and also not to create if it does not exist.'
            
            # drop the other column 
            if theSameWithThisSchema:
                for colName in crtColName:
                    self.execsql(\
                            "alter table " + tableName + " drop column if exists " + colName + " ;",
                            False,
                            {},
                            False
                        )

                    if self.__retStatus["state"] != "success":
                        warningFlag = 1
                        warningMsg = warningMsg + ' [drop column failure] ' + colName + ' ' + self.__retStatus["info"]
            
            if warningFlag == 1:
                self.__setStatus("warning",warningMsg,{})
            else:
                self.__setStatus("success","Alter table completely.",{})
    
    #
    # desc : drop table
    # param@tableName : table for droping
    # retn : None, call status() to get status object
    #
    def dropTable(self, tableName):
        if not (isinstance(tableName, str)):
            self.__setStatus("failure", "Parameters are not correct.", {})
            return    
    
        # check table status (whether it exists or not)
        try:
            self.execsql("select * from information_schema.tables where table_name = %(name)s;", True, {'name' : tableName})
        except:
            self.__setStatus("failure", "Can not get the table list.", {})
            return
            
        if self.__retStatus["state"] != "success":
            self.__setStatus("failure", "Can not check table status.", {})
            return

        if len(self.__retStatus["data"]) > 0:
            # table exist
            self.execsql("drop table if exists " + tableName + ";", False, {}, False)
            
            if self.__retStatus["state"] == "success":
                self.__setStatus("success", "Drop table " + tableName + " successfully.", {})
            else:
                self.__setStatus("failure", "Can not drop table " + tableName + ".", {})
        else:
            # table does not exist
            self.__setStatus("success", "Table " + tableName + " does not exist.", {})

class dataPeroidModel:
    
    # ---------------
    # private
    # ---------------
    __dbhost = ""
    __dbport = ""
    __dbname = ""
    __dbtable = ""
    __dbuser = ""
    __dbpass = ""
    
    def __status(self, state, info, data):
        return {"state" : state, "info" : info, "data" : data}
    
    # ---------------
    # public
    # ---------------
    
    #
    # desc : constructor
    #
    def __init__(self, dbhost, dbport, dbname, dbtable, dbuser, dbpass):
        self.__dbhost = dbhost
        self.__dbport = dbport
        self.__dbname = dbname
        self.__dbtable = dbtable
        self.__dbuser = dbuser
        self.__dbpass = dbpass
    
    #
    # desc : peroid count body
    #
    def countPeroidBody(self):
        p2l = py2psql.py2psql(\
            self.__dbhost, self.__dbport, \
            self.__dbname, self.__dbtable, \
            self.__dbuser, self.__dbpass\
        )
        
        p2l.execsql(\
            "select usrid, latest, (now()::date - latest::date) as day from download_summary;", \
            True,\
            {}\
        )
        
        retList = { "0-30" : 0, "31-90" : 0, "90-" : 0 }
        
        if p2l.status()["state"] == "success":
            
            for i in range(0, len(p2l.status()["data"]), 1):

                if (int)(p2l.status()["data"][i]["day"]) <= 30:
                    retList["0-30"] += 1
                elif (int)(p2l.status()["data"][i]["day"]) > 90:
                    retList["90-"] += 1
                else:
                    retList["31-90"] += 1

            return json.dumps(self.__status("success","Calculate peroids completely.",retList))
        
        else:
            
            return json.dumps(self.__status("failure","Can not calculate peroids.",retList))

class DownloadPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IRoutes)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
   
    #
    # desc : status object
    #
    def __status(self, state, info, data):
        return {"state" : state, "info" : info, "data" : data}

    # 
    # desc : create table for download_summary
    #
    def createTb(self):
        schemaFile = "ckanext/download/schema.json"
        dataSchema = ""
        if os.path.isfile(schemaFile):
            with open(schemaFile, "r") as fin:
                for line in fin:
                    dataSchema += line.strip()
            dataSchema = json.loads(dataSchema)

        p2l = None
        url = config.get('ckan.download.psqlUrl')
        pattern = re.compile('\S+://(\S+):(\S+)@(\S+):(\d+)/(\S+)')
        match = pattern.match(url)
        if match:
            link = pattern.findall(url)[0]
            p2l = py2psql.py2psql(link[2],link[3],link[4],"",link[0],link[1])
        else:
            pattern = re.compile('\S+://(\S+):(\S+)@(\S+)/(\S+)')
            link = pattern.findall(url)[0]
            p2l = py2psql.py2psql(link[2],"5432",link[3],"",link[0],link[1])

        p2l.createTable(\
            "download_summary", \
            dataSchema, \
            dropFirst=False \
        )
        if p2l.status()["state"] != "success":
            return self.__status("failure", p2l.status()["info"], {})
        else:
            return self.__status("success", "Create download_summary table completely.", {})

    # IConfigurer

    def update_config(self, config_):
        # replace package url
        template = plugins.toolkit.asbool(config.get('ckan.download.template'))
        if not (template == None or template == False):
            plugins.toolkit.add_template_directory(config_, 'templates')
        else:
            # keep counting download is available
            plugins.toolkit.add_template_directory(config, 'theme/templates')

        # new page
        plugins.toolkit.add_template_directory(config, 'theme/main')
        
        # add resource
        plugins.toolkit.add_resource('fanstatic', 'dwnres')

        if config.get('ckan.download.psqlUrl') == None:
            return "Error : Configuration is not set."
        
        if self.createTb()["state"] != "success":
            return "Error : The download_summary table can not be created. [info] : " + self.createTb()["info"]
       
    ## ITemplateHelpers
    
    def get_helpers(self):
        # define in the helpers.py
        return { 'counter' : counter, \
                 'countDwnBody' : countDwnBody, \
                 'countDownload' : countDownload, \
                 'getResDwnSummary' : getResDwnSummary, \
                 'transform2UTF8' : transform2UTF8, \
                 'getResSummaryInfo' : getResSummaryInfo, \
                 'getBothViewDwnCount' : getBothViewDwnCount, \
                 'getPkgSum' : getPkgSum, \
                 'getViewSum' : getViewSum, \
                 'getViewDwnCount' : getViewDwnCount \
               }
    
    def before_map(self, route_map):
        with routes.mapper.SubMapper(route_map, controller='ckanext.download.plugin:DownloadController') as m:
            m.connect('download_summary', '/download', action='download_summary')
            m.connect('download_date_summary', '/download_date', action='download_date_summary')
            m.connect('download_date_summary_peroid', '/download_date/peroid', action='download_date_summary_peroid')
        return route_map

    def after_map(self, route_map):
        return route_map


class DownloadController(base.BaseController):

    def __countRes(self, getResID):
        countDownload(getResID)

    def __renderUrl(self, option):
        tracking = plugins.toolkit.asbool(config.get('ckan.tracking_enabled'))
        if tracking == None or tracking == False:
            tracking = "False"
        else:
            tracking = "True"
        passVars = {"tracking" : tracking}
        if option == "index":
            return toolkit.render('index.html', extra_vars=passVars)
        elif option == "date":
            return toolkit.render('date.html', extra_vars=passVars)

    #
    # desc : controller for url path
    # 
    def download_summary(self):
        if plugins.toolkit.request.method == "GET" \
          and "resourceid" in plugins.toolkit.request.params.keys() \
          and "dataurl" in plugins.toolkit.request.params.keys():
            # count the resource
            self.__countRes(plugins.toolkit.request.params.get('resourceid'))

            # redirect to the data url
            #r = helpers.redirect_to(unicode(plugins.toolkit.request.params.get('dataurl')).encode('utf-8'))
            searchObj = re.search(r'.*download\?resourceid=.*&dataurl=(.*)', unicode(plugins.toolkit.request.url).encode('utf-8'), re.M | re.I)
            redirUrl = searchObj.groups()[0]
            r = redirect_to(redirUrl)
            return r
        else:
            return self.__renderUrl('index')

    def download_date_summary(self):
        return self.__renderUrl('date')


    def download_date_summary_peroid(self):

        url = config.get('ckan.download.psqlUrl')
        pattern = re.compile('\S+://(\S+):(\S+)@(\S+):(\d+)/(\S+)')
        match = pattern.match(url)
        if match:
            link = pattern.findall(url)[0]
            peroidObj = peroid.dataPeroidModel(link[2], str(link[3]), link[4], "download_summary", link[0], link[1])
            return peroidObj.countPeroidBody()
        else:
            pattern = re.compile('\S+://(\S+):(\S+)@(\S+)/(\S+)')
            link = pattern.findall(url)[0]
            peroidObj = peroid.dataPeroidModel(link[2], str("5432"), link[3], "download_summary", link[0], link[1])
            return peroidObj.countPeroidBody()










