#!/home/krwra/Applications/anaconda/bin/python

# THIS SCRIPT PROCESSES BOTH MAIN INDIVIDUAL UPDATES FROM KICKSTARTER AND OUTPUTS A CSV OF DETAILS

# Written by:  Kevin Williams
# Modified on: 2/1/2018


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
import bleach
import multiprocessing as mp
from functools import partial
from tqdm import *


######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain = "/mnt/data0/kickstarter2/data/"
pathOut = "/mnt/data0/kickstarter/data/clean/"
pathUpdateInd = pathMain+"updates_details/"


######################################################################
######### FUNCTION CALLS
######################################################################


# WRITE NEW MASTER FILE
def writeResults(X):
    g = open(pathOut + "projectUpdateData_2.csv", 'wb')
    csvWriter = csv.writer(g, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for x in X:
        if x != None:
            csvWriter.writerow(x)
    g.close()



# PARSE FILE TO OBTAIN PID AND TIMESTAMP TO STOP TRACKING
def parseFile(fname):
    f = open(fname)
    soup = BeautifulSoup(f,'html.parser')           # open the file and make some soup
    f.close()
    pidDetails = map(int, re.split("_",fname)[2:4])
    # try to obtain time stamp
    if len(soup.findAll("div", { "id" : "hidden_project" })) != 0:
        print "this project is subject to an IP dispute; ignore"
    else:
        try:
            updateNum	= int(str(soup.find("a", { "class" : "green no-wrap" }).contents[0]).replace("\n","").replace("Update #",""))
            updateTime 	= soup.find("p", { "class" : "published f6 grey-dark" }).find("time")['datetime']
            tsList = map(int, re.split("-|T|:", updateTime))
            try:
                numComments	= int(re.findall('\d+', str(soup.find("div", { "class" : "native-hide mobile-hide" }).find("a").contents[0]))[0])
            except AttributeError:
                numComments = int(re.findall('\d+', str(soup.find("span", { "class" : "comments" }).contents[0]))[0])    
            numLikes = soup.find("data", {"itemprop" : "Post[post_likes]"}).contents
            if numLikes == []:
            	likeCount = 0
            else:
            	likeCount = int(re.findall('\d+', str(numLikes[0]))[0])
            # extract the title of the post
            title 		= soup.find("meta", {"property" : "og:title"})["content"].replace("|","")
            descrip 	= re.sub('\s+', ' ',soup.find("meta", {"property" : "og:description"})["content"].replace("|",""))
            numFigures 	= len(soup.findAll("figure"))
            text		= soup.find("div", {"class" : "body readability responsive-media formatted-lists"})
            text = str(text).replace("<", " <")
            updateText  = re.sub(r'[^\w]', ' ', re.sub('\s+', ' ', bleach.clean(text, tags=[], strip=True)).replace("|",""))
            updateText  = ' '.join(updateText.split())
            success  	= True
        except Exception:
            print fname
            success = False
        if success == True:
            return pidDetails + tsList + [updateNum,numComments,likeCount,numFigures, len(title), len(descrip), len(updateText)] + [title,descrip,updateText]
        elif success == False:
            pass

def imap_unordered_bar(func, args, n_processes = 12):
    p = mp.Pool(n_processes)
    res_list = []
    with tqdm(total = len(args)) as pbar:
        for i, res in tqdm(enumerate(p.imap_unordered(func, args))):
            pbar.update()
            res_list.append(res)
    pbar.close()
    p.close()
    p.join()
    return res_list


if __name__ == '__main__':
    inds = glob.glob(pathUpdateInd + "*.html")
    result = imap_unordered_bar(parseFile, inds)
    writeResults(result)
    print "program complete"


