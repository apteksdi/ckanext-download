# -*- coding: utf-8 -*-
"""
Created on Thu Oct 20 12:52:23 2016

@author: JianKaiWang
"""

import py2psql
import json

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
    
    
    
    
    
    
