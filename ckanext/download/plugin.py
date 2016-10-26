import ckan.plugins as plugins
import ckan.lib.helpers as helpers
import ckan.plugins.toolkit as toolkit
import ckan.lib.base as base
import ckan.lib.render as render
import routes.mapper
import py2psql
import json
from pylons import config
import re
from helpers import * 
import peroid

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
          and len(plugins.toolkit.request.params) == 2 \
          and "resourceid" in plugins.toolkit.request.params.keys() \
          and "dataurl" in plugins.toolkit.request.params.keys():
            # count the resource
            self.__countRes(plugins.toolkit.request.params.get('resourceid'))

            # redirect to the data url
            r = helpers.redirect_to(unicode(plugins.toolkit.request.params.get('dataurl')).encode('utf-8'))
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










