#!/home/krwra/Applications/anaconda/bin/python

# THIS SCRIPT LOOKS AT THE EXISTING MASTER LIST AND ALL NEW FILES DOWNLOADED OVER THE PAST 25HRS
# IT THEN OUTPUTS THE MASTER PROJECT LIST, WITH A FLAG FOR DEAD PROJECTS

##### THIS PROGRAM GATHERS THE FRONT PAGE OF KICKSTARTER.COM
# Written by:  Kevin Williams
# Modified on: 3/8/2017

######################################
##### THIS SCRIPT RUNS AT 10:00:00AM and 10:00:00PM
##### AT MOST 5000 PAGES ARE DOWNLOADED AT ONE TIME (OR THE MAX # OBS ON KICKSTARTER)
######################################


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

######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
pathIn = "/mnt/data0/kickstarter2/data/existingprojects/"
pathMain = "/mnt/data0/kickstarter2/data/"

# this is the master listing file
master = pathMain + "masterListing.txt"

# define 24 hrs in the past
past = time.time() - 25*60*60 # 25 hours

# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Kickstarter scraping: successfully created master file"

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

# GLOB ALL FILES BEING DOWNLOADED
def globCurrent():
    files = glob.glob(pathIn + "Listing_*.html")
    return files

# EXTRACT PRODUCT ID FROM FILE
def extractPid(fname):
    li = re.split("_|\.", fname)
    return li[1]

# PARSE FILE TO OBTAIN PID AND TIMESTAMP TO STOP TRACKING
def parseFile(fname,pid):
    f = open(fname)
    soup = BeautifulSoup(f,'html.parser')			# open the file and make some soup
    f.close()
    print fname
    # try to obtain time stamp
    try:
        dataT=soup.find("p", { "class" : "mb3 mb0-lg type-12" })
        ts = dataT.time['datetime']      # extract timestamp for completion, such as 2017-04-06T10:04:06-04:00'
        tsList = re.split("-|T|:", ts)
    except Exception:
        print "error with time stamp in file " + fname
        tsList = ["2000","01","01","00","00","00","00","00","00"]   
    # try to obtain url for porject tracking
    try:     
        dataL = soup.find("meta", { "property" : "og:url" })
        ls = dataL['content']
    except Exception:
        print "error with project url in file " + fname
        ls = ""
    return [pid] + tsList + [ls]

# PARSE ALL FILES
def parseAll(Y):
    files = globCurrent()
    X = []
    for f in files:
        if os.path.getmtime(f) >= past:         # only work on files that have been created within 25 hrs
            pid = extractPid(f)                 # extract product id
            x = parseFile(f,pid)
            X.append(x)
    return X                            # return extended list of files tracked

# WRITE NEW MASTER FILE
def writeResults(X):
    g = open(master, 'wb')
    csvWriter = csv.writer(g, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for x in X:
   	csvWriter.writerow(x)
    g.close()

# CALCULATE THE MINIMUM DATE PER ID.
# THIS RESULTS IN A FLAG OF PROJECTS THAT HAVE COMPLETED.
def combineLists(X,Y):
    Z = X + Y
    df = pd.DataFrame(Z)
    df[10] = df[1]+":"+df[2]+":"+df[3]+":"+df[4]+":"+df[5]+":"+df[6]
    df[11] =  pd.to_datetime(df[10], format='%Y:%m:%d:%H:%M:%S')
    result=df.sort_values(11).groupby(0, as_index=False).last().drop([10,11], 1)
    return result.values.tolist()

######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    Y = openMaster(master)
    a= "number of files in original master: " + str(len(Y))
    print a
    X = parseAll(Y)
    b= "number of files in the past 25 hours: " + str(len(X))
    print b
    Z = combineLists(X,Y)
    c= "number of files in new master file: " + str(len(Z))
    print c
    writeResults(Z)
    d= "finished creating master file"
    print d
    TEXT = "\n".join([a,b,c,d])
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(LOGIN[0],LOGIN[1])
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    server.sendmail(FROM, TO, message)         
    server.quit()
