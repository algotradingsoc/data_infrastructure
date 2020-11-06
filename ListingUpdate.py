import pandas as pd

def ListingUpdate(day1,day2,method='finnhub_id'):
    #allternative method: by 'symbol'
    
    id1 = pd.read_csv(day1+'.csv')[method]
    id2 = pd.read_csv(day2+'.csv')[method]
    
    delisting_id = id1[id1.isin(id2)==False]
    listing_id = id2[id2.isin(id1)==False]
    
    return delisting_id,listing_id