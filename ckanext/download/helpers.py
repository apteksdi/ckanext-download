import py2psql
import threading
import datetime
from pylons import config
import re
import ckan.plugins as plugins

counter = threading.Lock()    

#
# desc : count download main body
# retn : status object {"state":"","info":"","data":{}}
#
def countDwnBody(getResID):
    global counter
    
    psqlInfo = getPSQLInfo()
    p2l = py2psql.py2psql(\
        psqlInfo['dbhost'], psqlInfo['dbport'], \
        psqlInfo['dbname'], psqlInfo['dbtable'], \
        psqlInfo['dbuser'], psqlInfo['dbpass'] \
    )
    
    # locking to prevent cs
    counter.acquire()
    
    # select to check resource status
    data = p2l.select({'usrid':getResID}, [], asdict=True)
    
    # not existing
    if len(data) < 1:
        # fetch resource table
        p2l_res = py2psql.py2psql(\
            psqlInfo['dbhost'], psqlInfo['dbport'], \
            psqlInfo['dbname'], "resource", \
            psqlInfo['dbuser'], psqlInfo['dbpass'] \
        )
        resInfo = p2l_res.select({'id':getResID},["package_id"],asdict=True) 
        
        # insert a new data
        if len(resInfo) > 0:
            p2l.insert({\
                "usrid":getResID, \
                "count":"1", \
                "latest": str(datetime.datetime.now()), \
                "dsid":resInfo[0]['package_id']
            })
            
            if p2l.status()['state'] == "success":
                status = {"state":"success","info":"Insert a new resource " + getResID + " count summary.","data":{}}
            else:
                status = {"state":"failure","info":"Can not insert a new resource " + getResID + " count summary.","data":{}}
        else:
            status = {"state":"failure","info":"Can not fetch resource " + getResID + " info.","data":{}}
    # existing
    else:
        # update status        
        # current count + 1
        addCount = (int)(data[0]['count']) + 1
        p2l.update({\
            "count":str(addCount), \
            "latest": str(datetime.datetime.now())
        },{"usrid":getResID})

        if p2l.status()['state'] == "success":
            status = {"state":"success","info":"Count a resource " + getResID + " successfully.","data":{}}
        else:
            status = {"state":"failure","info":"Can not count a resource " + getResID + ".","data":{}}

    # release the lock
    counter.release()
    return status
    
#
# desc : threading-based method to count the resource
#
def countDownload(getResID):
    t = threading.Thread(target=countDwnBody, args=(getResID,))
    t.start()


# 
# desc : transform unicode into UTF-8
# 
def transform2UTF8(getText):
    return unicode(getText, 'utf-8')

#
# desc : get psql server info
#
def getPSQLInfo():
    url = config.get('ckan.download.psqlUrl')
    pattern = re.compile('\S+://(\S+):(\S+)@(\S+):(\d+)/(\S+)')
    match = pattern.match(url)
    if match:
        link = pattern.findall(url)[0]
        return {\
            'dbhost':link[2], 'dbport':str(link[3]), \
            'dbname':link[4], 'dbtable':"download_summary", \
            'dbuser':link[0], 'dbpass':link[1]\
        }
    else:
        pattern = re.compile('\S+://(\S+):(\S+)@(\S+)/(\S+)')
        link = pattern.findall(url)[0]
        return {\
            'dbhost':link[2], 'dbport':str("5432"), \
            'dbname':link[3], 'dbtable':"download_summary", \
            'dbuser':link[0], 'dbpass':link[1]\
        }

#
# desc : get data for download summary
#
def getResDwnSummary(getResId):
    psqlInfo = getPSQLInfo()
    p2l = py2psql.py2psql(\
        psqlInfo['dbhost'], psqlInfo['dbport'], \
        psqlInfo['dbname'], psqlInfo['dbtable'], \
        psqlInfo['dbuser'], psqlInfo['dbpass'] \
    )
    sqlStr = """
		SELECT resource.*
			,package.NAME AS pkgname
			,package.title AS pkgtitle
			,package.id
		FROM (
			SELECT download_summary.count AS count
				,download_summary.latest AS latest
				,resource.id AS resid
				,resource.package_id
				,resource.format AS format
				,resource.NAME AS resName
				,resource.description
			FROM download_summary
			LEFT JOIN resource ON download_summary.usrid = resource.id """ 
    if getResId != "all":
        sqlStr += """   WHERE resource.id = %(getResId)s """
    sqlStr += """	) AS resource
		LEFT JOIN package ON resource.package_id = package.id;
    """
    
    if getResId != "all":
        p2l.execsql(sqlStr, True, {"getResId" : getResId})
    else:
        p2l.execsql(sqlStr, True, {})

    retList = []
    tmpTuple = ()

    if p2l.status()['state'] == "success":
        for index in range(0, len(p2l.status()['data']), 1):
            tmpTuple = (\
                        p2l.status()['data'][index]['resname'], \
                        p2l.status()['data'][index]['pkgname'], \
                        p2l.status()['data'][index]['count'], \
                        p2l.status()['data'][index]['latest'], \
                        p2l.status()['data'][index]['resid'], \
                        p2l.status()['data'][index]['id'], \
                        p2l.status()['data'][index]['pkgtitle'], \
                        str(-1) \
                       )
            retList.append(tmpTuple)
    
    return retList

#
# desc : summary the download count and include the view count
#
def getBothViewDwnCount(getResId):

    psqlInfo = getPSQLInfo()
    p2l = py2psql.py2psql(\
        psqlInfo['dbhost'], psqlInfo['dbport'], \
        psqlInfo['dbname'], psqlInfo['dbtable'], \
        psqlInfo['dbuser'], psqlInfo['dbpass'] \
    )
    sqlStr = """
	SELECT
		count, latest, resid, package_id, format, resname, description, resurl, pkgname, id,
		pkgtitle, max(resurltable_view) as rescnt, max(langurltable_view) as langcnt
	FROM
	(
		SELECT
			pkgres.*,
			resurltable.url as resurltable_url,
			resurltable.running_total as resurltable_view,
			langurltable.url as langurltable_url,
			langurltable.running_total as langurltable_view
		from
		(
			SELECT resource.*
				,package.NAME AS pkgname
				,package.title AS pkgtitle
				,package.id
			FROM (
				SELECT download_summary.count AS count
					,download_summary.latest AS latest
					,resource.id AS resid
					,resource.package_id
					,resource.format AS format
					,resource.NAME AS resName
					,resource.description
					,resource.url AS resurl
				FROM download_summary
				LEFT JOIN resource ON download_summary.usrid = resource.id """
    if getResId != 'all':
        sqlStr += """           WHERE resource.id = %(getResId)s """
    sqlStr += """		) AS resource
			LEFT JOIN package ON resource.package_id = package.id
		) as pkgres
		Left JOIN tracking_summary as resurltable ON pkgres.resurl = resurltable.url
		Left JOIN tracking_summary as langurltable ON '/dataset/' || pkgname || '/resource/' || resid = langurltable.url
	) as ressummaryinfo
	GROUP BY
		count, latest, resid, package_id, format, resname, description,
		resurl, pkgname, id, pkgtitle, resurltable_url, langurltable_url;
    """

    if getResId != 'all':
        p2l.execsql(sqlStr, True, {"getResId" : getResId})
    else:
        p2l.execsql(sqlStr, True, {})

    retList = []
    tmpTuple = ()

    if p2l.status()['state'] == "success":
        for index in range(0, len(p2l.status()['data']), 1):
            rescnt = p2l.status()['data'][index]['rescnt']
            langcnt = p2l.status()['data'][index]['langcnt']
            if rescnt == None:
                rescnt = 0
            if langcnt == None:
                langcnt = 0
            tmpTuple = (\
                        p2l.status()['data'][index]['resname'], \
                        p2l.status()['data'][index]['pkgname'], \
                        p2l.status()['data'][index]['count'], \
                        p2l.status()['data'][index]['latest'], \
                        p2l.status()['data'][index]['resid'], \
                        p2l.status()['data'][index]['id'], \
                        p2l.status()['data'][index]['pkgtitle'], \
                        str((int)(rescnt) + (int)(langcnt)) \
                       )
            retList.append(tmpTuple)

    return retList

#
# desc : get view and/or download count in resource page
# show : /dataset/ds/resource/resid
#
def getViewDwnCount(getResId):
    tracking = plugins.toolkit.asbool(config.get('ckan.tracking_enabled'))
    if tracking == None or tracking == False:
        return getResDwnSummary(getResId) 
    else:
        return getBothViewDwnCount(getResId)
        

#
# desc : calculate download summary information
#
def getResSummaryInfo():

    psqlInfo = getPSQLInfo()
    p2l = py2psql.py2psql(\
        psqlInfo['dbhost'], psqlInfo['dbport'], \
        psqlInfo['dbname'], psqlInfo['dbtable'], \
        psqlInfo['dbuser'], psqlInfo['dbpass'] \
    )
    p2l.execsql("""
	SELECT count(resource.id) AS ttlres
		,count(count) AS dwnres
	FROM resource
	LEFT JOIN download_summary ON resource.id = download_summary.usrid;
    """, True, {})
    
    tmpTuple = ()

    if p2l.status()['state'] == "success":
        tmpTuple = (\
            str(p2l.status()['data'][0]['ttlres']), \
            str(p2l.status()['data'][0]['dwnres']), \
            str((int)(p2l.status()['data'][0]['ttlres']) - (int)(p2l.status()['data'][0]['dwnres'])) \
        )

    retList = [tmpTuple]

    return retList

#
# desc : sum all resources count based on the same package
# 
def getPkgSum(getPkgId):
    psqlInfo = getPSQLInfo()
    p2l = py2psql.py2psql(\
        psqlInfo['dbhost'], psqlInfo['dbport'], \
        psqlInfo['dbname'], psqlInfo['dbtable'], \
        psqlInfo['dbuser'], psqlInfo['dbpass'] \
    )    
    p2l.execsql("""
	SELECT sum(count) AS total_count
	FROM download_summary
	WHERE dsid = '""" + getPkgId + """'
    """, True, {})
    if p2l.status()['state'] == "success":
        return str(p2l.status()['data'][0]["total_count"])
    else:
        return str(0)


#
# desc : show view count
#
def getViewSum(getPkgId):
    tracking = plugins.toolkit.asbool(config.get('ckan.tracking_enabled'))
    if tracking == None or tracking == False:
        return str(-1)

    psqlInfo = getPSQLInfo()
    p2l = py2psql.py2psql(\
        psqlInfo['dbhost'], psqlInfo['dbport'], \
        psqlInfo['dbname'], psqlInfo['dbtable'], \
        psqlInfo['dbuser'], psqlInfo['dbpass'] \
    )
    p2l.execsql("""
	SELECT package_id
		,sum(total_view) as total_view
	FROM (
		SELECT url
			,package_id
			,max(running_total) AS total_view
		FROM tracking_summary
		WHERE package_id = '""" + getPkgId + """'
		GROUP BY url
			,package_id
		) AS allData
	GROUP BY package_id;
    """, True, {})
    if p2l.status()['state'] == "success":
        if len(p2l.status()['data']) > 0:
            return str(p2l.status()['data'][0]["total_view"])
        else:
            return str(0)
    else:
        return str(-1)






