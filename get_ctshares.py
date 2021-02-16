# -*- coding: utf-8 -*-

# =============================================================================
# A function to query the Crowdtangle API and get URL shares
# =============================================================================
#  PARAMETERS
#   URL -> a Dataframe with URL and its published data
#   url_column -> name of the column that contains the URLs
#   date_column -> name of the column that countaines the published date of the URL
#   platforms -> platfrom to search on default "facebook,instagram" but only one can also be specified
#   nmax -> max results to be fetched default : 500 (API limit)
#   sleep_time -> pause betweet queries to respect API limits default : 20 sec
#   clean_urls -> clean the URLs from tracking parameters default: False
#   save_ctapi_output -> saves the original CT API output
# =============================================================================
# =============================================================================

#%%
import logging
import pandas as pd
import time
from datetime import timedelta
import requests
from tqdm import tqdm
from utils import clean_url
from pathlib import Path
logger = logging.getLogger(__name__)

#%%
def get_ct_shares(df_data,url_column='url', date_column='date', platforms=('facebook', 'instagram'), nmax=500, sleep_time=20, clean_urls=False, save_ctapi_output=False, API_KEY = None):
    ct_output = pd.DataFrame()
    
    if not API_KEY:
        message = "API_KEY not provided"
        raise Exception(message)
    
    if url_column not in df_data.columns:
        message = f"{url_column}not in data"
        raise Exception(message)
    
    if date_column not in df_data.columns:
        message = f"{date_column} not in data"
        raise Exception(message)
    
    df_data = df_data.drop_duplicates(subset=url_column, keep=False)
    logger.info("executing get_ctshares script\n\n")
    if clean_urls:
         df_data = clean_url(df_data, url_column)
         logger.info("Original URLs have been cleaned")
    
    
    for idx, rows in tqdm(df_data.iterrows(), total=df_data.shape[0]):
        try:
            start_time = time.strptime(rows[date_column],"%Y-%m-%dT%H:%M:%S")
            #set end time to 7days from the posted date
            end_time  = time.localtime(time.mktime(start_time) + 7*24*60*60) 
            end_time = time.strftime("%Y-%m-%dT%H:%M:%S", end_time)
            
            payload = {'link': rows[url_column], 'platforms': 'facebook', \
                       'startDate':rows[date_column].replace('T', ' '),\
                           'endDate':end_time.replace('T', ' '),\
                            'includeSummary': "false",\
                                'includeHistory':"true",\
                                    'sortBy':"date",\
                                        'token': API_KEY,\
                                            'count':nmax
                                            
                                
                       }
            r = requests.get("https://api.crowdtangle.com/links", params=payload)
            dict_resp = r.json()
            # print(dict_resp)
            # print(dict_resp['result']['pagination'].keys())
            
            if dict_resp['status'] != 200:
                logger.exception(f"Unexpected http response code on url {rows[url_column]}")
                # print("Unexpected http response code on url")
                continue
        
            #if data response is empty
            if not dict_resp['result']['posts']:
                # print("Empty response on url")
                logger.debug(f"Empty response on url: {rows[url_column]}")
                continue
            
            output = pd.DataFrame(dict_resp['result']['posts'])
            
            if 'nextpage' in dict_resp['result']['pagination'].keys():
                for url in dict_resp['result']['pagination']['nextpage']:
                    r = requests.get(url)
                    dict_response = r.json()
                    output = output.append(dict_response['result']['posts'], ignore_index=True)
                    time.sleep(sleep_time) # 20 seconds
            
            output['expanded'] = output['expandedLinks'].map(lambda x: x[0]).apply(pd.Series)['expanded']
            output.drop(['expandedLinks'], axis=1, inplace = True)
            
            # converting dict of dicts to pandas columns
            output['account'] = output['account'].apply(lambda x: {f'account.{k}': v for k, v in x.items()})
        
            account = output['account'].apply(pd.Series)
            
            output.drop(['account'], axis=1, inplace = True)
            
             
            statistics = output['statistics'].apply(pd.Series)
            actual = statistics['actual'].apply(lambda x: {f'statistics.actual.{k}': v for k, v in x.items()})
            actual = actual.apply(pd.Series)
            expected = statistics['expected'].apply(lambda x: {f'statistics.expected.{k}': v for k, v in x.items()})
            expected = expected.apply(pd.Series)
            
            output.drop(['statistics'], axis=1, inplace = True)
           
            output_full = pd.concat([output, account, actual, expected], axis=1)
            output_full['date'] = pd.to_datetime(output_full['date'])
            output_full = output_full.set_index('date', drop=False)
            
            # remove links that are after 1 week end-date
            output_full = output_full.loc[(output_full.index <= output_full.index.min()+ pd.Timedelta('7 day'))]
        
            ct_output = ct_output.append(output_full, ignore_index=True)
        
            #clean variables
            del output
            del output_full
            
            time.sleep(sleep_time)
        except Exception as e:
            logger.exception(f"error on {rows['Article_Analized']} \n\n")
            logger.info(f"ADDITIONAL EXCEPTION INFO \n\n: {e}")
            
            # print("Error in {}".format(rows['Article_Analized']))
        
    if ct_output.empty: # if no CTshares are found
        logger.error("No ct_shares were found!")
        raise SystemExit("\n No ct_shares were found!")
        
    # save intermediate file here
    if save_ctapi_output:
        #create dir to save raw data
        Path("rawdata").mkdir(parents=True, exist_ok=True)
        # save raw dataframe
        ct_output.to_csv('rawdata/ct_shares_df.csv', index=False)
    
    ct_output = ct_output[ct_output['account.url'] != "https://facebook.com/null"] # remove broken urls
    
    ct_output.drop_duplicates(subset= ["id", "platformId", "postUrl", "expanded"],\
                                inplace=True, ignore_index = True)
        
    #Clean URLS here
    if clean_urls:
        ct_output = clean_url(ct_output, "expanded")
        logger.info("expanded URLs have been cleaned")
        
    
    
    
    #Calculate is original
    ct_output['is_orig'] = ct_output["expanded"].apply(lambda x: bool(df_data[url_column].str.contains(x, case=False, regex=False).sum()))
    
    #log stats
    logger.info(f"Original URLs: {len(df_data)}")
    logger.info(f"Crowdtangle shares: {len(ct_output)}")
    uni = len(ct_output["expanded"].unique())
    logger.info(f"Unique URL in Crowdtangle shares: {uni}")
    sum_accu = sum(ct_output["account.verified"])
    logger.info(f"Links in CT shares matching original URLs: {sum_accu}")
    
    
    return ct_output
