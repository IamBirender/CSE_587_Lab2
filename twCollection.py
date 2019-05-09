import keyring
import twitter as tw
import numpy as np
import json
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import datetime
import os.path

# %%
# Configure API

api = tw.Api( consumer_key=keyring.get_password("twitterCSE487_consumer","mpbrown"), consumer_secret=keyring.get_password("twitterCSE487_consumersecret","mpbrown"), access_token_key=keyring.get_password("twitterCSE487_accesstoken","mpbrown"), access_token_secret=keyring.get_password("twitterCSE487_accesstokensecret","mpbrown"), sleep_on_rate_limit=True, tweet_mode='extended')



# %%
# Set keywords
keywords = ['bike', 'bus', 'car', 'pedestrian', 'light rail','plane', 'train', 'subway', 'walk']


#%%
# Configure query
startDate = "2019-04-11"
endDate = "2019-04-18"
dateRange = pd.date_range(start=startDate,end=endDate, closed='left', freq='D')

for date in dateRange:
    sinceDate = date.strftime('%Y-%m-%d')
    untilDate = (date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# %%
api = tw.Api( consumer_key=keyring.get_password("twitterCSE487_consumer","mpbrown"), consumer_secret=keyring.get_password("twitterCSE487_consumersecret","mpbrown"), access_token_key=keyring.get_password("twitterCSE487_accesstoken","mpbrown"), access_token_secret=keyring.get_password("twitterCSE487_accesstokensecret","mpbrown"), sleep_on_rate_limit=True, tweet_mode='extended')
for date in dateRange:
    sinceDate = date.strftime('%Y-%m-%d')
    untilDate = (date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    twCount = 100
    
    for k in ['pedestrian','light rail', 'subway', 'walk']:
        tw_dict = {'tweetID':[], 'text':[]}
        lowestID = None
        counter = 0
        keylimit = 10000
        initialDict = 0
        if k in ['light rail', 'pedestrian', 'walk', 'subway']:
            keylimit = 2000
        
        while twCount > 0 and len(tw_dict['tweetID']) < keylimit and counter < 30:
            try:
                if lowestID == None:
                    # First pass
                    print(str(counter)+' - Querying TW for '+k+' since '+sinceDate+' until '+untilDate)
                    print('Timestamp at ' +(str(datetime.datetime.now())))
                    results = api.GetSearch(term=k+' -filter:retweets', since=sinceDate, until=untilDate, count=900,lang='en', return_json='true', result_type='recent')
                else:
                    print(str(counter)+' - Querying TW for '+k+' since '+sinceDate+' until '+untilDate+' with max_id '+str(lowestID))
                    print('Timestamp at ' +(str(datetime.datetime.now())))
                    results = api.GetSearch(term=k+' -filter:retweets', since=sinceDate, until=untilDate, count=900,lang='en', return_json='true', result_type='recent', max_id=lowestID)
                
                for s in results['statuses']:
                    if lowestID == None:
                        lowestID = s['id']
                    if not 'retweeted_status' in s:
                        tweetID = s['id']
                        text = s['full_text']
                        
                        if not "@subway" in text.lower():
                            tw_dict['tweetID'] = tw_dict['tweetID'] + [tweetID]
                            tw_dict['text'] = tw_dict['text'] + [text]
                            
                            if tweetID < lowestID:
                                lowestID = tweetID
    
                twCount = len(results['statuses'])
                counter = counter + 1
                print('tw_dict contains '+str(len(tw_dict['tweetID'])))
                                
                tw_df = pd.DataFrame(tw_dict)
                filename = 'data/tw/fullText/'+k+'.csv'
                if k in ['light rail']:
                    filename = 'data/tw/fullText/lightrail.csv'
                if os.path.isfile(filename):
                    existing_df = pd.read_csv(filename, index_col=0)
                    tw_df = pd.concat([existing_df, tw_df], ignore_index=True)
                tw_df = tw_df.drop_duplicates(subset='tweetID')
                print('Saving tw_dict to '+filename)
                tw_df.to_csv(filename)
                
                if len(tw_dict['tweetID']) - initialDict < 2:
                    break
                else:
                    initialDict = len(tw_dict['tweetID'])
            except tw.api.TwitterError as err:
                print('Error!')
                if err['code'] and err['message']:
                    print(str(err['code'])+' error: '+err['message'])
                time.sleep(901)


# %%
# Group themed datasets
a_df = pd.read_csv('data/tw/fullText/subway.csv', index_col=0)
b_df = pd.read_csv('data/tw/fullText/lightrail.csv', index_col=0)
c_df = pd.concat([a_df, b_df], ignore_index=True)
c_df = c_df.drop_duplicates(subset='tweetID')
c_df.to_csv('data/tw/grouped/lightrail.csv')

a_df = pd.read_csv('data/tw/fullText/pedestrian.csv', index_col=0)
b_df = pd.read_csv('data/tw/fullText/walk.csv', index_col=0)
c_df = pd.concat([a_df, b_df], ignore_index=True)
c_df = c_df.drop_duplicates(subset='tweetID')
c_df.to_csv('data/tw/grouped/pedestrian.csv')

for k in ['bike', 'bus', 'car','plane', 'train']:
    a_df = pd.read_csv('data/tw/fullText/'+k+'.csv', index_col=0)
    a_df.to_csv('data/tw/grouped/'+k+'.csv')
    

# %%

# Count number of grouped tweets

totalTweets = 0
for k in ['bike', 'bus', 'car', 'pedestrian', 'lightrail','plane', 'train']:
    a_df = pd.read_csv('data/tw/grouped/'+k+'.csv', index_col=0)
    print(k+' '+str(a_df.size))
    totalTweets += a_df.size
    
print(str(totalTweets))
    
# %%

























