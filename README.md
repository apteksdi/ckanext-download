# ckanext-download

The extension provides a way to summarize the resource download event. Customized template snippets and summary information pages are also supported.


# Requirements

The extension is compatible with CKAN core 2.3 or later.


# Configuration

Set the configuration to use the plugin :

```ini
ckan.plugins = download

# postgresql database to record downloading information
ckan.download.psqlUrl = postgresql://(dbuser):(dbpass)@(dbhost)/(dbname)
```


# Resource Summary

After installing the extension, the url path **/download** and **/download_date** on the browser show the download summary.

* url **/download**

![/download](https://raw.githubusercontent.com/jiankaiwang/ckanext-download/master/doc/image/download.png)

* url **/download_date**

![/download_date](https://raw.githubusercontent.com/jiankaiwang/ckanext-download/master/doc/image/download_date.png)


# Template snippets

The extension also customizes the template on several sections.

* on dataset, url **/dataset**

![/dataset](https://raw.githubusercontent.com/jiankaiwang/ckanext-download/master/doc/image/dataset.png)

* on dataset info, url **/dataset/(dataset)** 

![/dataset/(dataset)](https://raw.githubusercontent.com/jiankaiwang/ckanext-download/master/doc/image/datasetinfo.png)

* on resource, url **/dataset/(dataset)/resource/(id)**

![/dataset/(dataset)/resource/(id)](https://raw.githubusercontent.com/jiankaiwang/ckanext-download/master/doc/image/resource.png)

Set the configuration to customize the template.

```ini
# postgresql database to record downloading information
ckan.download.template = true
```


# Traking_summary supported

* The extension also supports the default view tracking in the ckan (2.3 or later).
* Follow the [page (http://docs.ckan.org/en/latest/maintaining/tracking.html)](http://docs.ckan.org/en/latest/maintaining/tracking.html) to setup view tracking.
* The extension recognizes the configuration below :

```ini
[app:main]
ckan.tracking_enabled = true
```


# Development Installation


To install ckanext-download for development, activate your CKAN virtualenv and do :

```bash
git clone https://github.com/jiankaiwang/ckanext-download.git
cd ckanext-download
python ./setup.py develop

# run in the develop environment
# assume the development.ini located at /etc/ckan/default
paster serve /etc/ckan/default/development.ini
```


# Installation


To install ckanext-download:

1. Activate your CKAN virtual environment, for example::

```bash
. /usr/lib/ckan/default/bin/activate
```

2. Install the ckanext-download Python package into your virtual environment::
```bash
git clone https://github.com/jiankaiwang/ckanext-download.git
cd ckanext-download
python setup.py install
```

3. Add ``download`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with nginx on Ubuntu::
```bash
# restart ckan service
sudo service ckan restart

# restart server service
sudo service nginx restart
```


# 3rd Party Javascript Libraries

* jquery.tablesorter : v.2.0.3
* jquery : v.1.12.4
* plotly : v.1.16.2


