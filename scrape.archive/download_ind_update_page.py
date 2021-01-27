#!/home/krwra/Applications/anaconda/bin/python

# THIS SCRIPT GRABS ALL UPDATES FOR ANNOUNCEMENTS GATHERED IN UPDATES_MASTER

##### THIS PROGRAM GATHERS THE FRONT PAGE OF KICKSTARTER.COM
# Written by:  Kevin Williams
# Modified on: 1/25/2018




######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
from bs4 import BeautifulSoup
import unicodedata
import datetime
import re
import glob
import csv
import os
import numpy as np
import time
import pandas as pd
import smtplib
import requests
import random




######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
pathMain = "/mnt/data0/kickstarter2/data/"
pathIn = pathMain+"updates_main/"
pathOut = pathMain+"updates_details/"


# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Kickstarter scraping: successfully scraped project updates"

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

proxy_host = "<<REMOVED>>"
proxy_port = "<<REMOVED>>"
proxy_auth = "<<REMOVED>>:"  # Make sure to incl



######################################################################
######### DEFINE FUNCTIONS
######################################################################


# REQUEST SPECIFIC URL
def requestWeb(url):
    pg = "https://www.kickstarter.com" + url
    user_agent ={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #user_agent ={'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
    r = requests.get(pg, headers = user_agent)
    return r

# REQUEST SPECIFIC URL
def requestWebProxy(url):
    pg = "https://www.kickstarter.com" + url
    proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
               "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}
    r = requests.get(pg, proxies=proxies,
                     verify=False)
    return r


# WRITE PROJECT DETAILS TO FILE
def writeResults(pid,pid_alt, post_id, content,timeStamp):
    # write results to file
    with open(pathOut + "Update_" + pid + "_" + post_id + "_" + timeStamp + ".html", 'w') as f:
        f.write(content)       
    print "page write complete, " + pid + "_" + pid_alt + "_" + post_id + "_" + timeStamp


# DOWNLOAD UPDATE PAGE PER PID
def downloadUpdatePages(fname,alreadyDownloaded):
    pid = re.split("_", fname)[2]
    print "working on pid: " + pid
    f = open(fname)
    soup = BeautifulSoup(f,'html.parser') 
    f.close()
    dataT=soup.findAll("a", { "class" : "grid-post link hover-target" })
    if len(dataT) != 0:
        for it in range(len(dataT)):
            try:
                time.sleep(random.uniform(0,1))     # go to sleep for a little bit
                url = str(dataT[it]['href'])
                pid_alt = re.split("/",url)[2]
                post_id = re.split("/",url)[-1]
                if "Update_" + pid + "_" + post_id in alreadyDownloaded:
                    print "already downloaded"
                else:
                    r=requestWeb(url)           # download page
                    source = r.text        # obtain page source
                    content = unicodedata.normalize('NFKD', source).encode('ascii','ignore') # convert unicode to ascii
                    timeStamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')        # record time stamp
                    writeResults(pid,pid_alt, post_id, content,timeStamp) 
            except AttributeError:
                print "UNVALID LINK FOR PID " + pid
    elif len(dataT) == 0:
        print "this project did not have any updates"


######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    counter = 0
    files = glob.glob(pathIn + "*")
    alreadyDownloaded = [re.split("\.|/",f)[6] for f in glob.glob(pathOut + "*")]
    alreadyDownloaded = ["_".join(re.split("_",f)[0:3]) for f in alreadyDownloaded]
    for fname in files:
        print "Currently " + str(round(100*float(counter)/len(files),2)) + "\% done with data collection"
        downloadUpdatePages(fname,alreadyDownloaded)
        counter += 1

# dup drop 1371814343
