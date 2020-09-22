"""
    Clean History HTML Files To Create allSuccessfulProjects
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Tues 22 Sep 2020
-------------------------------------------------------------------------------
notes:

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
import json
import unicodedata
import datetime
import re
import glob
import csv
import os
import numpy as np
import time
import pandas as pd
from multiprocessing import Pool, freeze_support


######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain 		                       = "/gpfs/project/kickstarter/"
pathIn 			                       = pathMain + "kickstarter_history/data/"
pathOut 		                       = pathMain + "agg/"

# extract the key details of the history file
def parseFile(fname):
    with open(fname, "r") as data_file:    
        data                           = json.load(data_file)
    
    dataParsed                         = [data['backers_count'],
                                        data['created_at'],
                                        data['deadline'],
                                        data['goal'],
                                        data['id'],
                                        data['launched_at'],
                                        data['usd_pledged'],
    ]
    
    dataParsed.extend([str(data['urls']['web']['project'])])
    
    return dataParsed

# process all files with pool and create the df
def processFiles():
    jsonFiles 		                    = [y for x in os.walk(pathIn) for y in glob.glob(os.path.join(x[0], '*.json'))]
    jsonFiles.pop(0) # this first file is data__kickstarter__com.json; ignore
    pool 				                = Pool()
    results 			                = pool.map(parseFile, jsonFiles)
    pool.close()
    pool.join()
    
    df 				                    = pd.DataFrame(np.array(results))
    df.rename(columns={
        0:'backers_count',
        1:'created_at',
        2:'deadline',
        3:'goal',
        4:'pid',
        5:'launched_at',
        6:'usd_pledged',
        7:'url'
        },inplace=True)
    df.loc[:,df.columns != 'url']       = df.loc[:,df.columns != 'url'].apply(pd.to_numeric)
    
    df['created_at'] 		            = df['created_at'].apply(datetime.datetime.fromtimestamp)
    df['deadline'] 			            = df['deadline'].apply(datetime.datetime.fromtimestamp)
    df['launched_at'] 		            = df['launched_at'].apply(datetime.datetime.fromtimestamp)
    df['year'] 				            = df.launched_at.dt.year
    df['month'] 			            = df.launched_at.dt.month
    
    return df


# RUN THE MAIN PROGRAM
if __name__ == '__main__':
    df                                  = processFiles()
    print("processing complete")
    df                                  .to_csv(pathOut+"allSuccessfulProjects.csv", sep='|')






