"""
    Download Main Updates From allSuccessfulProjects
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Tues 22 Sep 2020
-------------------------------------------------------------------------------
notes:
    Do not run this code; the API code has been removed; it was written in Py2.7; it is for archiving how to download main updates.
--------------------------------------------------------------------------------
contributors:
    Kevin:
        name:       Kevin Williams
        email:      kevin.williams@yale.edu
--------------------------------------------------------------------------------
Copyright 2020 Yale University
"""

######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
import unicodedata
import datetime
from bs4 import BeautifulSoup
import csv
import re
import numpy as np
import requests
import time
import random
import smtplib
import os
import pandas as pd
import multiprocessing as mp
from functools import partial
from tqdm import *


from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp   = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
pathMain    = "/mnt/data0/kickstarter_history/"
pathIn      = pathMain + "data/"
pathOut     = pathMain+"updates_main/"

proxy_host  = "<<URL REMOVED>>"
proxy_port  = "<<PORT REMOVED>>"
proxy_auth  = "<<KEY_REMOVED>>:"  # Make sure to incl :

######################################################################
######### DEFINE FUNCTIONS
######################################################################

# OPEN CURRENT MASTER FILE
def openMaster():                       # open project details
    df = pd.read_csv(pathMain + "allSuccessfulProjects.csv", sep='|')
    return df

# REQUEST SPECIFIC URL
def requestWeb(url):
    pg              = url + "/updates"
    user_agent      ={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    proxies         = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                        "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}
    r               = requests.get(pg, proxies=proxies,
                     verify=False)
    #r = requests.get(pg, headers = user_agent)
    return r

# WRITE PROJECT DETAILS TO FILE
def writeResults(pid,contents,timeStamp):
    # write results to file
    with open(pathOut + "Listing_" + pid + "_" + timeStamp + "project_updates.html", 'w') as f:
        f.write(contents)       
    #print "page write complete, " + pid +", " + timeStamp


# MOVE FILES FOR ALL PROJECTS GREATER THAN A WEEK OLD
def downloadUpdatePages(i,df):
    row             = df.iloc[i]
    pid             = str(int(row['id']))                          # extract project id
    url             = row['url']                         # extract url to download
    try:
        r           = requestWeb(url)           # download page
        source      = r.text        # obtain page source
        content     = unicodedata.normalize('NFKD', source).encode('ascii','ignore') # convert unicode to ascii
        timeStamp   = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')        # record time stamp
        writeResults(pid,content,timeStamp) 
    except Exception:
        print "UNVALID LINK FOR PID: " + pid


# DEFINE MP OVER PIDS. IT WILL ALSO DISPLAY PROGRESS BAR
def imap_unordered_bar(df,func, args, n_processes = 23):
    
    p               =       mp.Pool(n_processes)
    processP        =       partial(func , df = df)
    res_list        =       []
    
    with tqdm(total = len(args)) as pbar:
        for i, res in tqdm(enumerate(p.imap_unordered(processP, args))):
            pbar.update()
            res_list.append(res)
    pbar.close()
    p.close()
    p.join()
    return res_list

######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    df = openMaster()
    #downloadUpdatePages(df)
    result          =   imap_unordered_bar(df , downloadUpdatePages , range(df.shape[0]) )   # MP OVER Y
    print "program complete"






