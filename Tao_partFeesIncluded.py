#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Mon May  8 14:29:55 2017

@author: Tao Su, uku.ele@gmail.com

This program uses FIFO(First in, first out) method to compute stock return.

(FIFO: If you buy shares of a stock at different points in time, and then sell 
some of those shares, then you would typically assume that you sold your 
longest held stock first.)

Either long a stock or short a stock needs to buy and sell some stocks. In
transaction records, the total quantities of buyings and sellings are not 
necessarily the same. The difference between the two, i.e., the extra buyings
or sellings are not computed because we don't know the exact return of them. 
Accordingly, their corresponding parts of trasanction fees are not included 
in the computation.

"""


import numpy as np
import pandas as pd


#Function stock_benefit is to compute someone's benefit of the same stock. Here 
#the total numbers of buying and selling have to be equal. 
def stock_benefit(buyQ,buyP,buyF,sellQ,sellP,sellF):
    """ Return the benefit of a given stock. """
    sumBuy=np.sum(buyQ*(buyP+buyF))
    sumSell=np.sum(sellQ*(sellP-sellF))
    return sumSell-sumBuy

    
def benefit(buys,buyPrices,buyFees,sells,sellPrices,sellFees):
    """Process the record series and compute the benefits."""
    buyCount=np.sum(buys)
    sellCount=np.sum(sells)
    
    #If there is only buying or selling, there is no benefit
    if buyCount*sellCount==0:
        return 0
    
    #Find the sellings and buyings with the same length, and call stock_benefit
    if buyCount>=sellCount :
        n=np.cumsum(buys)
        edg=np.where(n>=sellCount)[0][0]
        rem=buys[edg]-n[edg]+sellCount
        res=np.hstack((buys[:edg],np.array([rem])))
        return stock_benefit(res,buyPrices[:edg+1],buyFees[:edg+1],sells,
                             sellPrices,sellFees)
    else:
        n=np.cumsum(sells)
        edg=np.where(n>buyCount)[0][0]
        rem=sells[edg]-n[edg]+buyCount
        res=np.hstack((sells[:edg],np.array([rem])))
        return stock_benefit(buys,buyPrices,buyFees,res,sellPrices[:edg+1],
                             sellFees[:edg+1])


def stock_FIFO(inputFile,outputFile):
    """ The functon to compute stock returns."""
    
    #The tags are not very well aligned, we have to manually set them. All 
    #records are read into the dataframe 'records'
    cols=['OrderId','Trader','StkCode','Quantity','Price','TradeType','Fee',
    'Date','Time']
    records=pd.read_table(inputFile, skiprows=[0],header=None,names=cols)
    
    #We drop the time information which doesn't affect the caculation. And then
    #the records are grouped by the traders' names, stocks' codes and trades' 
    #types and coverted to records2.
    records.drop(['Date','Time'],axis=1,inplace=True)
    
    #Average the transaction fee to every stock
    records['Fee']=records['Fee']/records['Quantity']
    records2=records.set_index(['Trader','StkCode','TradeType']).sort_index()
    
    #records3 is a dictionary to save results
    records3={}
    
    
    #Loop for all users to get their total earnings
    for l1 in np.unique(records2.index.get_level_values('Trader')):
        earnings=0
        #Loop for every stock to get its related benefits
        for l2 in np.unique(records2.ix[l1].index.get_level_values('StkCode')):
            #Get the subgroup of every stock records
            sgroup=records2.ix[l1,l2]
            #Get the buying series
            if 'Buy' in sgroup.index:
                sbuy=records2.ix[l1,l2,'Buy']['Quantity']
                sbuyPrice=records2.ix[l1,l2,'Buy']['Price']
                sbuyFee=records2.ix[l1,l2,'Buy']['Fee']
                if(np.size(sbuy)==1):
                    sbuy=np.resize(sbuy,1)
                    sbuyPrice=np.resize(sbuyPrice,1)
                    sbuyFee=np.resize(sbuyFee,1)
                else:
                    sbuy=sbuy.as_matrix()
                    sbuyPrice=sbuyPrice.as_matrix()
                    sbuyFee=sbuyFee.as_matrix()
            else:
                sbuy=np.array([])
                sbuyPrice=np.array([])
                sbuyFee=np.array([])
            #Get the selling series    
            if 'Sell' in sgroup.index:           
                ssell=records2.ix[l1,l2,'Sell']['Quantity']
                ssellPrice=records2.ix[l1,l2,'Sell']['Price']
                ssellFee=records2.ix[l1,l2,'Sell']['Fee']
                if(np.size(ssell)==1):
                    ssell=np.resize(ssell,1)
                    ssellPrice=np.resize(ssellPrice,1)
                    ssellFee=np.resize(ssellFee,1)
                else:
                    ssell=ssell.as_matrix()
                    ssellPrice=ssellPrice.as_matrix()
                    ssellFee=ssellFee.as_matrix()
            else:
                ssell=np.array([])
                ssellPrice=np.array([])
                ssellFee=np.array([])
                
            theBenefit=benefit(sbuy,sbuyPrice,sbuyFee,ssell,ssellPrice
                               ,ssellFee)
            #Collect all the earnings
            earnings=earnings+theBenefit
        #Benefits of every trader are saved into a dictionary
        records3[l1]=earnings
    
    #Covert the dictionary to a series and sort.
    totalBenefit=pd.Series(records3)
    totalBenefit.sort_values(ascending=False,inplace=True)
    
    #Write the results to the output file using given format
    totalBenefit.to_csv('output.tsv',sep='\t',float_format = '%.2f')
    print(totalBenefit)