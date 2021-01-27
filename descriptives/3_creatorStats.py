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

import pandas as pd
from difflib import SequenceMatcher
from fuzzywuzzy import fuzz
from scipy import stats


def similar(a, b):
    return fuzz.partial_ratio(a, b)


pathMain                               = "/mnt/data0/kickstarter/"
#pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "agg/"
pathOut                                = pathMain + "output/"
pathPlot                               = pathOut  + "plots/" 
pathStats                              = pathOut  + "sumStats/" 
pathRepdata                            = pathOut  + "repData/"


df1 					= pd.read_csv(pathIn + "kaggle/ks-projects-201801.csv")
df1.rename(columns={"ID" : "pid"}, inplace=True)

df2 					= pd.read_csv(pathIn + "creatorList.csv")
df 						= df1.merge(df2, on="pid", how="inner")
df["numJ"] 				= df.groupby("creator")["pid"].transform("count")
df 						= df.loc[df.numJ >= 2]

df["catJ"] 				= df.groupby(["creator", "main_category"])["pid"].transform("count")
df 						= df.loc[df.catJ >= 2]

df["deadline"] 			= pd.to_datetime(df.deadline)


df 						= df.sort_values(["creator", "deadline"]).reset_index(drop=True)


df["ones"] 				= 1
df['index'] 			= df.groupby('creator')['ones'].transform(pd.Series.cumsum)

df 						= df.loc[df.numJ==2]
c1 						= df.loc[(df["index"] == 1) & (df.state == "failed")]["creator"].reset_index(drop=False)
c2 						= df.loc[(df["index"] == 2)]["creator"].reset_index(drop=False)
df_m 					= df.merge(c1["creator"], on = "creator", how="inner")
df_m 					= df_m.merge(c2["creator"], on = "creator", how="inner")
df_m["launched"] 		= pd.to_datetime(df_m.launched)
df_m["length"] 			= df_m.deadline - df_m.launched
# note that 2 comes after 1

df_m 					= df_m[["creator", "goal", "deadline", "launched", "length", "index", "name", "state"]]
df_m["index"] 			= df_m["index"].astype("str")

res 					= df_m.pivot(index='creator', columns='index', values=["goal", "length", "name", "state"])
res.columns 			= res.columns.map(''.join)
res["ratio"] 			= res.apply(lambda row : similar(row['name1'], row['name2']), axis = 1)
res 					= res.loc[res["ratio"] >= 99]

res["goalDif"] 			= (res.goal2 - res.goal1).astype("float")
res["lenDif"] 			= (res["length2"].astype('timedelta64[D]') - res["length1"].astype('timedelta64[D]'))
res["goalfrac"] 		= (res.goal2/res.goal1).astype("float")

resF = res.loc[res.state2 == "failed"].reset_index(drop=True)
resS = res.loc[res.state2 == "successful"].reset_index(drop=True)



sumFile     			= pathStats + 'creator_match'
logger      			= open(sumFile + ".log","w")

logger.write("exercise of creator matching results" + "\n")

logger.write(	str(res.shape)				)
logger.write(	"\ngoaldif (after - before) for successful\n"			)
logger.write(	resS.goalDif.describe().to_string()			)
logger.write(	"\nlenDif (after - before for successful)\n"			)
logger.write(	resS.lenDif.describe().to_string()					)
logger.write(	"\ngoalfrac for successful\n"			)
logger.write(	resS.goalfrac.describe().to_string()					)

logger.write(	"\ngoaldif (after - before) for unsuccessful\n"			)
logger.write(	resF.goalDif.describe().to_string()			)
logger.write(	"\nlenDif (after - before for unsuccessful)\n"			)
logger.write(	resF.lenDif.describe().to_string()					)
logger.write(	"\ngoalfrac for unsuccessful\n"			)
logger.write(	resF.goalfrac.describe().to_string()					)

logger.write("\ntest length for successful\n")
logger.write(	str(stats.ttest_rel(resS.length1, resS.length2))					)
logger.write("\ntest goal for successful\n")
logger.write(	str(stats.ttest_rel(resS.goal1, resS.goal2))				)

logger.write("\ntest length for unsuccessful\n")
logger.write(	str(stats.ttest_rel(resF.length1, resF.length2))					)
logger.write("\ntest goal for unsuccessful\n")
logger.write(	str(stats.ttest_rel(resF.goal1, resF.goal2))				)

logger.close()