#!/home/krwra/Applications/anaconda/bin/python

# THIS SCRIPT LOOKS AT THE EXISTING MASTER LIST, PAST PROJECTS, AND DOWNLOADS THE MAIN UPDATE PAGE

##### THE MAIN UPDATE PAGE IS AT KICKSTARTER.COM/PROJECT/UPDATE
# Written by:  Kevin Williams
# Modified on: 1/15/2018


######################################################################
######### IMPORT REQUIRED PACKAGES
######################################################################
from selenium import webdriver
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
import glob


### this flag will restrict downlodas to new projects only
downloadAll = False


######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
pathIn = "/mnt/data0/kickstarter2/data/existingprojects/"
pathMain = "/mnt/data0/kickstarter2/data/"
pathOut = pathMain+"updates_main/"

# this is the master listing file
master = pathMain + "masterListing.txt"

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Kickstarter scraping: successfully scraped project updates"


proxy_host = "<<REMOVED>>"
proxy_port = "<<REMOVED>>"
proxy_auth = "<<REMOVED>>:"  # Make sure to incl


######################################################################
######### DEFINE FUNCTIONS
######################################################################

# OPEN CURRENT MASTER FILE
def openMaster(master):
    if os.path.isfile(master) == False:
        with open(master, "w+") as f:
            print "master missing, create masterListing"
            X = []
    else:
        print "loading master file"
        with open(master, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter="|")
            X = []
            for row in reader:
                X.append(row)
    return X                            # create masterListing or blank list


# REQUEST SPECIFIC URL
def requestWeb(url):
    pg = url + "/updates"
    user_agent ={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #user_agent ={'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
    r = requests.get(pg, headers = user_agent)
    return r

# REQUEST SPECIFIC URL
def requestWebProxy(url):
    pg = url + "/updates"
    proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
               "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}
    r = requests.get(pg, proxies=proxies,
                     verify=False)
    return r


# WRITE PROJECT DETAILS TO FILE
def writeResults(pid,contents,timeStamp):
    # write results to file
    with open(pathOut + "Listing_" + pid + "_" + timeStamp + "project_updates.html", 'w') as f:
        f.write(contents)       
    print "page write complete, " + pid +", " + timeStamp


# MOVE FILES FOR ALL PROJECTS GREATER THAN A WEEK OLD
def downloadUpdatePages(Y,existingFiles,downloadAll):
    for y in Y:
        checker  = datetime.date(int(y[1]), int(y[2]), int(y[3])) < datetime.date.today() - datetime.timedelta(weeks=1)
        checker2 = y[0] in existingFiles
        if downloadAll == True : checker2 = False
        if (checker == True) and (checker2 == False):
            #time.sleep(random.uniform(0,1))     # go to sleep for a little bit
            pid = y[0]                          # extract project id
            url = y[-1]                         # extract url to download
            try:
                r=requestWebProxy(url)           # download page
                source = r.text        # obtain page source
                content = unicodedata.normalize('NFKD', source).encode('ascii','ignore') # convert unicode to ascii
                timeStamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')        # record time stamp
                writeResults(pid,content,timeStamp) 
            except Exception:
                print "UNVALID LINK FOR PID: " + pid
        else:
            print "this project is still current"


######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    Y = openMaster(master)
    existingFiles = list(set([re.split("_",f)[2] for f in glob.glob(pathOut + "*.html")]))
    downloadUpdatePages(Y,existingFiles,downloadAll)
    c= "collection of project update pulls complete"
    print c
    TEXT = "\n ".join([c])
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(LOGIN[0],LOGIN[1])
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    server.sendmail(FROM, TO, message)         
    server.quit()
