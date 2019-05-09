import keyring
import twitter as tw
import pandas as pd
import time
import datetime
import os.path

# %%
# Set keywords
keywords = ['bike', 'bus', 'car', 'pedestrian', 'light rail','plane', 'train', 'subway', 'walk']
keywords = ['sample']

# %%
# Configure query date range
startDate = "2019-04-20"
endDate = "2019-04-21"

# %%
# Configure Twitter API with extended mode to get full text
api = tw.Api( consumer_key=keyring.get_password("twitterCSE487_consumer","mpbrown"), consumer_secret=keyring.get_password("twitterCSE487_consumersecret","mpbrown"), access_token_key=keyring.get_password("twitterCSE487_accesstoken","mpbrown"), access_token_secret=keyring.get_password("twitterCSE487_accesstokensecret","mpbrown"), sleep_on_rate_limit=True, tweet_mode='extended')

# Loop through data range
for date in pd.date_range(start=startDate,end=endDate, closed='left', freq='D'):
    # Formats date
    sinceDate = date.strftime('%Y-%m-%d')
    untilDate = (date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    twCount = 100
    
    # Loop through keywords
    for k in keywords:
        # tw_dict will be converted into a pandas DataFrame, used to store tweet text and tweetID
        tw_dict = {'tweetID':[], 'text':[]}
        # Counters to keep track for API query
        lowestID = None
        counter = 0
        keylimit = 10000
        initialDict = 0
        if k in ['light rail', 'pedestrian', 'walk', 'subway']:
            keylimit = 2000
        if k in ['sample']:
            keylimit = 250
        
        while twCount > 0 and len(tw_dict['tweetID']) < keylimit:
            try:
                if lowestID == None:
                    # First pass
                    print(str(counter)+' - Querying TW for '+k+' since '+sinceDate+' until '+untilDate)
                    print('Timestamp at ' +(str(datetime.datetime.now())))
                    
                    results = api.GetSearch(term=k+' -filter:retweets', since=sinceDate, until=untilDate, count=900,lang='en', return_json='true', result_type='recent')
                else:
                    print(str(counter)+' - Querying TW for '+k+' since '+sinceDate+' until '+untilDate+' with max_id '+str(lowestID))
                    print('Timestamp at ' +(str(datetime.datetime.now())))
                    # After first pass, uses max_id for keeping track
                    results = api.GetSearch(term=k+' -filter:retweets', since=sinceDate, until=untilDate, count=900,lang='en', return_json='true', result_type='recent', max_id=lowestID)
                
                # Loops through each tweet
                for s in results['statuses']:
                    if lowestID == None:
                        lowestID = s['id']
                    # Filters out any retweets
                    if not 'retweeted_status' in s:
                        tweetID = s['id']
                        text = s['full_text']
                        
                        if not "@subway" in text.lower(): # Filters out tweets from subway restaurant
                            # Saves ID and text to tw_dict
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
    # Groups 'subway' and 'lightrail' datasets
    # Groups 'pedestrian' and 'walk' datasets
    # For all other keywords, just saves csv to the 'data/tw/grouped/' folder
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
# For internal use
# Count number of grouped tweets
totalTweets = 0
for k in ['bike', 'bus', 'car', 'pedestrian', 'lightrail','plane', 'train']:
    a_df = pd.read_csv('data/tw/grouped/'+k+'.csv', index_col=0)
    print(k+' '+str(a_df.size))
    totalTweets += a_df.size
    
print(str(totalTweets))
    
# %%

























