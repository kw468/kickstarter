#!/home/krwra/Applications/anaconda/bin/python

##### THIS PROGRAM CREATES A FILE OF THE MOST RECENT NEWPROJECT DOWNLOADS
##### FROM KICKSTARTER.COM
# Written by:  Kevin Williams
# Modified on: 3/8/2017

######################################
##### THIS SCRIPT RUNS AT 1:00:30AM and 1:00:30PM,
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
import time
import os
import smtplib


######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathIn = "/mnt/data0/kickstarter2/data/newprojects/parsed/"
pathMain = "/mnt/data0/kickstarter2/data/"

# define 24 hrs in the past
past = time.time() - 24*60*60 # 24 hours

# this is the master listing file
master = pathMain + "newListing.txt"

# define email log in
FROM = "<<REMOVED>>"
TO = "<<REMOVED>>"
LOGIN = ["<<REMOVED>>", "<<REMOVED>>"]
SUBJECT = "Kickstarter scraping: successfully created initial listings file"


######################################################################
######### DEFINE FUNCTIONS
######################################################################

# OPEN NEWLISTING CSV
def openCSV(fname):
    with open(fname, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter="|")
        X = []
        for row in reader:
            X.append(row)
    return X  

# GLOB FILES IN PREVIOUS 24 HOURS
def globCurrent():
    files = glob.glob(pathIn+"newProjectsListings_*")          # gather all files in directory
    X = []
    for f in files:
        if os.path.getmtime(f) >= past:
            x = openCSV(f)
            X.extend(x)
    Y = [x[0:3] for x in X]
    uniqueListings = [list(x) for x in set(tuple(x) for x in Y)]
    return uniqueListings

# OPEN CURRENT NEW LISTING FILE
def openMaster(master):
    if os.path.isfile(master) == False:
        with open(master, "w+") as f:
            print "master missing, create NewListing"
            X = []
    else:
        print "loading newListing file"
        with open(master, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter="|")
            X = []
            for row in reader:
                X.append(row)
    return X  

# WRITE NEW NEWLISTING FILE TO FILE
def writeResults(X):
    g = open(master, 'wb')
    csvWriter = csv.writer(g, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for x in X:
        csvWriter.writerow(x)
    g.close()

######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    X = openMaster(master)
    Y = globCurrent()
    a = "There are " + str(len(Y)) + " projects that have started in the past 24 hrs"
    Z = [list(x) for x in set(tuple(x) for x in X + Y)]
    b = "With new pulls and existing master file, " + str(len(Z)) + " projects are in the data set"
    writeResults(Z)
    c = "Successfully created new listing files"
    TEXT = "\n".join([a,b,c])
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login(LOGIN[0],LOGIN[1])
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    server.sendmail(FROM, TO, message)         
    server.quit()
