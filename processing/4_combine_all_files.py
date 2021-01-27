"""
    Combines All Cleaned PID CSVs To Single Parquet
    Kickstarter Project
--------------------------------------------------------------------------------
change log:
    v0.0.1  Thurs 24 Sep 2020
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

import os
import pandas as pd
import glob
import shutil
from scipy import stats
import numpy as np

######################################################################
######### DEFINE PRIMATIVES
######################################################################
pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "projects/cleaned/"
pathOut                                = pathMain + "agg/"

######################################################################
######### DEFINE FUNCTIONS
######################################################################

# RUN THE MAIN PROGRAM
if __name__ == '__main__':
    files = glob.glob(pathIn + "*.csv")
    files = [f for f in files if os.path.getsize(f) != 1]
    name = 'temp.csv'
    with open(pathOut + name, 'w') as outfile:
        for i, fname in enumerate(files):
            with open(fname, 'r') as infile:
                infile.readline()  # Throw away header on all files
                # Block copy rest of file from input to output without parsing
                shutil.copyfileobj(infile, outfile)
                print(fname + " has been imported.")
    df = pd.read_csv(pathOut + name, header = None, sep = "|", dtype={'1': 'float64',
                                              '2' : 'float64',
                                              '3' : 'float64',
                                              '4' : 'float64',
                                              '5' : 'float64',
                                              '6' : 'float64',
                                              '7' : 'float64',
                                              '8' : 'float64',
                                              '9' : 'float64',
                                              '10' : 'float64',
                                              '11' : 'float64',
                                              '12' : 'str',
                                              '13' : 'float64',
                                              '14' : 'float64',
                                              '15' : 'float64',
                                              '16' : 'float64',
                                              '17' : 'float64',
                                              '18' : 'float64',
                                              '19' : 'float64',
                                              '20' : 'float64',
                                              '21' : 'str',
                                              '22' : 'float64',
                                              '23' : 'float64',
                                              '24' : 'float64',
                                              '25' : 'float64',
                                              '26' : 'float64',
                                              '27' : 'float64',
                                              '28' : 'float64',
                                              })
    #df[18] = df[18].replace({',':''},regex=True).apply(pd.to_numeric,1)
    #df.rename(columns=lambda x: int(x), inplace=True)
    df.rename(columns={ 0:'pid',
                        1:'t',
                        2:'edate_year',
                        3:'edate_month',
                        4:'edate_day',
                        5:'edate_hour',
                        6:'edate_min',
                        7:'sdate_year',
                        8:'sdate_month',
                        9:'sdate_day',
                        10:'sdate_hour',
                        11:'sdate_min',
                        12:'category',
                        13:'cumfracG',
                        14:'goal',
                        15:'cumTotalRev',
                        16:'length',
                        17:'cumBackerRev',
                        18:'cumTotalN',
                        19:'cumBackerN',
                        20:'avgBackerRev',
                        21:'bucketCap',
                        22:'bucketPrice',
                        23:'bucketBackerN',
                        24:'rewardId',
                        25:'currencyConv',
                        26:'love',
                        27: 'htmlType',
                        28: 'add_on',
                        }, inplace=True)
    # drop any duplicates we might have
    df                              = df.drop_duplicates().reset_index(drop=True)
    # adjust for exchange rate fluc
    df["currency"]                  = df.groupby("pid")["currencyConv"].transform(lambda x: stats.mode(x)[0][0]) 
    # adjust for goal differences that stem from html errors in html type = 3, and very few of type = 2 have errors.
    # errors are such as: 4.400 and 4 400 -> 4.4, and 4 in cleaning
    checker = len(df.loc[df.htmlType == 1].groupby("pid").goal.nunique().value_counts())
    if checker == 1:
      print("goals unique within htmlType = 1")
    else:
      print("non-uniqueness of goals within htmlType = 1")
    df1                             = df.loc[df.htmlType == 1].groupby("pid")["goal"].agg(lambda x: stats.mode(x)[0][0]) 
    df1                             = df1.reset_index(drop = False)
    # reset goal and cumfracG
    df                              .drop(columns="goal", inplace = True)
    df                              = df.merge(df1, on = "pid", how="inner")
    # because of goal changes, we must adjust cumFracG
    df["cumfracG"]                  = df.cumTotalRev/df.goal
    # sometimes length is not defined and we do not remove these rows. Make adjustment here because length is constant and we only every observe -1 and truth
    checker                         = df.groupby("pid").length.unique().reset_index()
    checker                         = checker.loc[checker.length.str.len() > 1]
    checker                         = checker.length.values
    checker                         = [np.sum(c[0] != -1) for c in checker]
    if np.max(checker) == 1:
      print("goal length is unique")
    else:
      print("non-uniqueness in length of projects")
    df["length"]                    = df.groupby("pid").length.transform("max") # this addresses -1 when page changes
    name = 'masterListings.parquet.gzip'
    df.to_parquet(pathOut + name, compression = "gzip", engine = "pyarrow")
    name = 'temp.csv'
    os.remove(pathOut + name)
