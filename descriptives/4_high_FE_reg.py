"""
    Runs High Dim FE Model On d_R And b_R
--------------------------------------------------------------------------------
change log:
    v0.0.1  Sun 27 Sep 2020
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
from sklearn.linear_model import LinearRegression
from scipy import sparse
from sklearn.preprocessing import OneHotEncoder
from scipy.sparse import csr_matrix
import numpy as np


pathMain                               = "/home/kw468/Projects/kickstarter/"
#pathMain                               = "/gpfs/project/kickstarter/"
pathIn                                 = pathMain + "agg/"
pathOut                                = pathMain + "output/"
pathPlot                               = pathOut  + "plots/" 
pathStats                              = pathOut  + "sumStats/" 
pathRepdata                            = pathOut  + "repData/"



shipAdjust = "US"
logFile = True

df2 = pd.read_parquet(pathRepdata + "kickstarter_DF2"+shipAdjust+".parquet.gzip")


sumFile         = pathStats + 'high_dim_fe'
# turn on logging or not:
if logFile:
    logger      = open(sumFile + "_" + shipAdjust + ".log","w")


# this converts the main df2 into the estimation df
def createSample(df2):
    df3                 = df2.loc[df2.successEver == 1].reset_index(drop=True)
    df3                 = df3[["pid", "relativeHit", "d_R", "d_N", "b_R", "b_N", "successEver"]]
    df3["maxRT"]        = df3.groupby("pid").relativeHit.transform("max")
    df3["minRT"]        = df3.groupby("pid").relativeHit.transform("min")
    #
    df3                 = df3.loc[df3.maxRT >= 3]
    df3                 = df3.loc[df3.minRT <= -3]
    df3                 = df3.loc[df3.relativeHit != 0]
    #
    df3                 = df3.reset_index(drop=True)
    #
    df3["after"]        = df3.relativeHit.values > 0
    df3["after"]        = df3["after"].astype("float")
    return df3


# simple comparison of means, pre-post
def runPrePostMeans(df3):
    means               = df3.groupby(["pid", "after"]).d_R.mean().reset_index(drop=False)
    means               = pd.pivot_table(means, values='d_R', index=['pid'],
                                        columns=['after'], aggfunc=np.sum)
    #
    #
    sum1                = (means[0] > means[1]).sum()
    sum2                = (means[0] == means[1]).sum()
    uniqueJ             = means.shape[0]
    #
    if logFile:
        logger.write("Number of Projects in Sample: {:10.0f}".format(uniqueJ) + "\n")
        logger.write("Number of Projects where PreMean > PostMean: {:10.0f}".format(sum1) + "\n")
        logger.write("Number of Projects where PreMean == PostMean Sample: {:10.0f}".format(sum2) + "\n")



# this creates a before and after high dim FE model; the outcome are the coefs. This is for D_R
def run_highFE_Reg_DR(df):
    y                       = df.pid.values
    after 	                = df.after.values
    enc1 	                = OneHotEncoder(drop = "first")
    drop_enc1               = enc1.fit( (y * (1 - after)).reshape(-1, 1))
    X1                      = drop_enc1.transform((y * (1 - after)).reshape(-1, 1))
    enc2 	                = OneHotEncoder(drop="first")
    drop_enc2               = enc2.fit( (y * (after)).reshape(-1, 1))
    X2                      = drop_enc2.transform((y *(after)).reshape(-1, 1))
    X                       = sparse.hstack((X2, X1 ))
    reg                     = LinearRegression(fit_intercept=False).fit(X, df.d_R.values)
    return reg.coef_	


# this constructs a bootstrap sample for D_R regression
def runOne_DR(df3,l):
    df4 = df3.copy()
    df4 = df4.groupby(["pid", "after"]).d_R.sample(frac=1, replace=True).reset_index(drop=False)
    df4 = df4.merge(df3[["pid", "after"]],  left_on = "index", right_index = True)
    print("iter " + str(l)  + " complete")
    return run_highFE_Reg_DR(df4)


# this creates a before and after high dim FE model; the outcome are the coefs. This is for B_R
def run_highFE_Reg_BR(df):
    y                       = df.pid.values
    after                   = df.after.values
    enc1                    = OneHotEncoder(drop = "first")
    drop_enc1               = enc1.fit( (y * (1 - after)).reshape(-1, 1))
    X1                      = drop_enc1.transform((y * (1 - after)).reshape(-1, 1))
    enc2                    = OneHotEncoder(drop="first")
    drop_enc2               = enc2.fit( (y * (after)).reshape(-1, 1))
    X2                      = drop_enc2.transform((y *(after)).reshape(-1, 1))
    X                       = sparse.hstack((X2, X1 ))
    reg                     = LinearRegression(fit_intercept=False).fit(X, df.b_R.values)
    return reg.coef_    

# this constructs a bootstrap sample for B_R regression
def runOne_BR(df3,l):
    df4 = df3.copy()
    df4 = df4.groupby(["pid", "after"]).b_R.sample(frac=1, replace=True).reset_index(drop=False)
    df4 = df4.merge(df3[["pid", "after"]],  left_on = "index", right_index = True)
    print("iter " + str(l) + " complete")
    return run_highFE_Reg_BR(df4)


# this runs iters bootstrap samples of the high dim FE model and outputs results for the paper
def runBootStrap(df3):
    z1 = 1.282 ; z5 = 1.645 ; z01 = 2.34
    Z = [z1,z5,z01]
    nJ = df3.pid.nunique()
    iters = 500
    # run the model
    results_DR = [runOne_DR(df3,l) for l in range(iters)]
    thetaD = [r[:nJ] for r in results_DR]
    thetaD = np.array(thetaD)
    thetabarD = thetaD.mean(axis=0)
    SE_D = np.sqrt(1/(iters-1) * ((thetaD - thetabarD[None,:])**2).sum(axis=0))
    betahat_D = df3.loc[df3.after == 1].groupby("pid").d_R.mean().values
    T_D = betahat_D / SE_D
    #
    if logFile:
        logger.write("Fraction of Rejections at 10%: {:10.10f}".format(sum(T_D > z1)/nJ) + "\n")
        logger.write("Fraction of Rejections at 5%: {:10.10f}".format(sum(T_D > z5)/nJ) + "\n")
        logger.write("Fraction of Rejections at 1%: {:10.10f}".format(sum(T_D > z01)/nJ) + "\n")


# run the main program
if __name__ == '__main__':
    df3                 = createSample(df2)
    runPrePostMeans(df3)
    runBootStrap(df3)
    #
    if logFile:
        logger.close()



# results_BR = [runOne_BR(df3,l) for l in range(iters)]
# thetaB = [r[:nJ] for r in results_BR]
# thetaB = np.array(thetaB)
# thetabarB = thetaB.mean(axis=0)
# SE_B = np.sqrt(1/(iters-1) * ((thetaB - thetabarB[None,:])**2).sum(axis=0))
# betahat_B = df3.loc[df3.after == 1].groupby("pid").b_R.mean().values
# T_B = betahat_B / SE_B

# for z in Z:
#     print(sum(T_B > z))