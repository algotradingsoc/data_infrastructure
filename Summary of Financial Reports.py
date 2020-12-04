##################################################
##  see 'concepts' in the financial reports as  ##
##   standardised categories for information    ##
##################################################

import json
import os
from os import listdir
from os.path import isfile, join


#available information ('concept') for a particular year & quarter
def RepSum(year, QTR, path = 'D:/ReportedFinancials'):
    
    mfile_path = path+'/'+year+'.'+QTR
    mfile_name = [f for f in listdir(mfile_path) if isfile(join(mfile_path, f))]
    
    msummary = dict()
    
    #open the financial report for each company in the specified year & quarter
    for filename in mfile_name:
        
        path = mfile_path+'/'+filename
        with open(path) as f:
            data = json.load(f)
        
        msym = data['symbol'] #symbol for the company
        mdat = data['data']
        
        bs = mdat['bs']
        cf = mdat['cf']
        ic = mdat['ic']
        
        bs_concept = set(d['concept'] for d in bs) #all concepts in bs report
        cf_concept = set(d['concept'] for d in cf) #all concepts in cf report
        ic_concept = set(d['concept'] for d in ic) #all concepts in ic report
        
        msummary[msym] = {'bs':bs_concept,'cf':cf_concept,'ic':ic_concept}
    
    return msummary


#all available information ('concept') for the duration specified
def CollateSummary(start_year = 2009, end_year = 2020):
    
    year_range = [str(s) for s in range(start_year,end_year+1)]
    QTR_range = ['QTR' + str(s) for s in range(1,5)]
    
    dsummary = dict()
    
    for year in year_range:
        for QTR in QTR_range:
            
            try:
                dsummary[year+QTR] = RepSum(year, QTR)
                
            except:
                print('Data for '+year+QTR+' is missing.')
    
    return dsummary


all_summary=CollateSummary() #get a summary of the available data in financial reports 
#structure of 'all_summary': 
#year.QTR -> symbol -> types of reports (bs, cf, ic) -> available information ('concept') in the report


#######################################################################################################

#all possible concepts in different types of financial reports (bs, cf, ic)
def CollateConcepts(start_year = 2009, end_year = 2020, path = 'D:/ReportedFinancials'):
    year_range = [str(s) for s in range(start_year,end_year+1)]
    QTR_range = ['QTR' + str(s) for s in range(1,5)]
    
    bs_concept = set()
    cf_concept = set()
    ic_concept = set()
    
    for year in year_range:
        for QTR in QTR_range:
            
            mfile_path = path+'/'+year+'.'+QTR
            
            #if data is available for the specified year & quarter
            if os.path.exists(mfile_path):
                
                mfile_name = [f for f in listdir(mfile_path) if isfile(join(mfile_path, f))]
                
                for filename in mfile_name:
                    path = mfile_path+'/'+filename
                    with open(path) as f:
                        data = json.load(f)

                    mdat = data['data']

                    bs = mdat['bs']
                    cf = mdat['cf']
                    ic = mdat['ic']

                    bs_concept |= set(d['concept'] for d in bs)
                    cf_concept |= set(d['concept'] for d in cf)
                    ic_concept |= set(d['concept'] for d in ic)
                    
    return bs_concept, cf_concept, ic_concept


#all available concepts in bs, cf, ic during the period 2009-2020
bs,cf,ic = CollateConcepts()

