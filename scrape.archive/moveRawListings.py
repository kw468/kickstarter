#!/home/krwra/Applications/anaconda/bin/python

# THIS SCRIPT LOOKS AT THE EXISTING MASTER LIST AND MOVES ALL COMPLETED PROJECTS TO A DIFFERENT FOLDER

##### THIS PROGRAM GATHERS THE FRONT PAGE OF KICKSTARTER.COM
# Written by:  Kevin Williams
# Modified on: 5/15/2017


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
import shutil

######################################################################
######### DEFINE PRIMATIVES
######################################################################
timeStamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
pathIn = "/mnt/data0/kickstarter2/data/existingprojects/"
pathMain = "/mnt/data0/kickstarter2/data/"
pathOut = "/mnt/data0/kickstarter2/data/projects/raw/"

# this is the master listing file
master = pathMain + "masterListing.txt"

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

# MOVE FILES FOR ALL PROJECTS GREATER THAN A WEEK OLD
def moveFiles(Y):
    for y in Y:
        checker=datetime.date(int(y[1]), int(y[2]), int(y[3])) < datetime.date.today() - datetime.timedelta(weeks=1)
        if checker == True:
            files = glob.glob(pathIn + "*"+y[0]+"*")
            print "number of files for this project are: " + str(len(files))
            for f in files:
                newName = f.replace("existingprojects", "projects/raw")
                shutil.move(f,newName)
        else:
            print "this project is still current"


######################################################################
######### EXECUTE
######################################################################
if __name__ == "__main__":
    Y = openMaster(master)
    moveFiles(Y)
