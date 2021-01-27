#!/home/krwra/Applications/anaconda/bin/python

#!/anaconda/bin/python
##### THIS PROGRAM GATHERS THE INITIAL DOWNLOAD FOR EACH PROJECT ON KICKSTARTER
##### IT DOWNLOADS EACH FILE TO HTML
# Written by:  Kevin Williams
# Modified on: 3/8/2017


######################################
##### THIS SCRIPT RUNS AT 6:00:00AM and 6:00:00PM
##### AT MOST 100 PAGES ARE DOWNLOADED AT ONE TIME (OR THE MAX # NEW OBS ON KICKSTARTER)
######################################


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


######################################################################
######### DEFINE PRIMATIVES
######################################################################
mainUrl = "https://www.kickstarter.com"
pathOut = "/mnt/data0/kickstarter2/data/existingprojects/"
pathMain = "/mnt/data0/kickstarter2/data/"

masterE = pathMain + "masterListing.txt"
masterN = pathMain + "newListing.txt"

# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Kickstarter scraping: successfully scraped initial projects"


######################################################################
######### DEFINE FUNCTIONS
######################################################################

# OPEN LISTING CSV
def openCSV(fname):
    with open(fname, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter="|")
        X = []
        for row in reader:
            X.append(row)
    return X 

def determineDownloads(masterE,masterN):
    listE = openCSV(masterE)
    listN = openCSV(masterN)
    if listE != []:
        existingListings = np.array(listE)[:,0]
    else:
        existingListings = []
    return [l for l in listN if l[1] not in existingListings]

# REQUEST SPECIFIC URL
def requestWeb(url):
    user_agent ={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #user_agent ={'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}
    r = requests.get(url, headers = user_agent)
    return r

# WRITE PROJECT DETAILS TO FILE
def writeResults(pid,contents,timeStamp):
	# write results to file
	with open(pathOut + "Listing_" + pid + "_" + timeStamp + ".html", 'w') as f:
		f.write(contents)		
	print "page write complete, " + pid +", " + timeStamp

# DOWNLOAD ALL LISTINGS
def downloadAll(listings):
    for x in listings:
        time.sleep(random.uniform(0,2))     # go to sleep for a little bit
    	pid = x[1]                          # extract project id
    	url = x[2]                         # extract url to download
        print "working on " + pid
    	r=requestWeb(url)			# download page
    	source = r.text        # obtain page source
    	content = unicodedata.normalize('NFKD', source).encode('ascii','ignore') # convert unicode to ascii
        timeStamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')        # record time stamp
    	writeResults(pid,content,timeStamp)                                      # write results to file


######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    listings = determineDownloads(masterE,masterN)
    a= "Need to download " + str(len(listings)) + " listings"
    b= "Starting the download process"
    print a
    print b
    downloadAll(listings)
    c= "collection of initial project pulls complete"
    print c
    TEXT = "\n".join([a,b,c])
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(LOGIN[0],LOGIN[1])
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    server.sendmail(FROM, TO, message)         
    server.quit()
