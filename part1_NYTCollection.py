import keyring
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup

# %%
# Set keywords
keywords = ['bike','bus','car','train', 'subway', 'light rail', 'bus', 'plane', 'pedestrian', 'walk']
urls = ['bike','bus','car','lrsubway','pedestrianwalk','plane','subway','train']

# %%
for k in keywords:
    
    # Initialize empty dictionary for keywords and urls, to later convert to pandas dataframe
    url_dict = {'keywords':[], 'urls':[]}
    
    # Initialize control counters
    pagenum = 0
    current_offset = 0
    maxhits = 10
     
    # Loop through response for all hits, limit up to 100
    while maxhits - current_offset >= 10 and current_offset < 100:
        # Makes sure NYT API doesn't hit rate limit
        print('Sleeping for 7 seconds')
        time.sleep(7)
        
        print('Querying NYT on '+k+' with page '+str(pagenum))
        
        # Configure query payload below
        payload = {'begin_date':'20190101','end_date':'20190408','q':k,'api-key':keyring.get_password('nyt','mpbrown'),'page':pagenum,'fq':'source:("The New York Times")ANDnews_desk.contains("Environment" "Editorial" "Cars" "Business" "Flight" "Home" "Metro" "Metropolitan" "Opinion" "OpEd" "Politics" "Society" "Technology" "The City" "Travel")'}
        # Send API request
        response = requests.get('https://api.nytimes.com/svc/search/v2/articlesearch.json', params=payload)
        # Convert to JSON
        data = response.json()
        
        # Handle errors based on status code
        status_code = response.status_code
        if status_code == 200:
            print("Query successful with "+str(data['response']['meta']['hits'])+' hits')
        elif status_code == 401:
            print("401: Unauthorized requests. Make sure api-key is set")
        elif status_code == 429:
            print('Too many requests. You reached the per day rate limit')
        else:
            print('Unknown error')           
        
        # Save URLs by looping through each web url
        print('Caching URLs to url_dict')
        for u in range(len(data['response']['docs'])):
            url = data['response']['docs'][u]['web_url']
            url_dict['keywords'] = url_dict['keywords'] + [k]
            url_dict['urls'] = url_dict['urls'] + [url]
        
        # Update control counters
        maxhits = data['response']['meta']['hits']
        current_offset = data['response']['meta']['offset']
        pagenum = pagenum + 1
        
        # Save requests to dataframe
        url_df = pd.DataFrame(url_dict)
        filename = 'data/nyt/urls/ungrouped/' + k + '.csv'
        if k in ['light rail']:
            filename = 'data/nyt/urls/ungrouped/lightrail.csv'
        
        # Remove any duplicated urls
        url_df = url_df.drop_duplicates(subset='urls')
        print('Saving url_dict to '+filename)
        url_df.to_csv(filename)
        

# %%
# Transforms ungrouped CSVs into grouped CSVs
# groups 'subway' and 'lightrail' into 'subway'
# groups 'pedestrian' and 'walk' into 'pedestrian'
a_df = pd.read_csv('data/nyt/urls/ungrouped/subway.csv', index_col=0)
b_df = pd.read_csv('data/nyt/urls/ungrouped/lightrail.csv', index_col=0)
c_df = pd.concat([a_df, b_df], ignore_index=True)
c_df = c_df.drop_duplicates(subset='urls')
c_df.to_csv('data/nyt/urls/lightrail.csv')

a_df = pd.read_csv('data/nyt/urls/ungrouped/pedestrian.csv', index_col=0)
b_df = pd.read_csv('data/nyt/urls/ungrouped/walk.csv', index_col=0)
c_df = pd.concat([a_df, b_df], ignore_index=True)
c_df = c_df.drop_duplicates(subset='urls')
c_df.to_csv('data/nyt/urls/pedestrian.csv')

for k in ['bike', 'bus', 'car','plane', 'train']:
    a_df = pd.read_csv('data/nyt/urls/ungrouped/'+k+'.csv', index_col=0)
    a_df.to_csv('data/nyt/urls/'+k+'.csv')
    

# %%
# Web scrape the NYT article paragraphs based on their URL
url_keywords = ['bike','bus','car','lightrail','pedestrian','plane','train']

for k in url_keywords:
    url_filename = 'data/nyt/urls/' + k + '.csv'
    article_filename = 'data/nyt/articles/' + k + '.csv'
    print('Reading '+url_filename)
    url_df = pd.read_csv(url_filename, index_col=0)
    art_dict = {'text':[]}
    
    for url in url_df['urls']:
        print('Sending req for '+url)
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        articleBody = soup.find('section', attrs={ 'name':'articleBody'})
            
        if articleBody is not None:
            paragraphs = articleBody.find_all('p')
            for p in paragraphs:
                art_dict['text'] = art_dict['text'] + [p.text]
    
    art_df = pd.DataFrame(art_dict)
    art_df.to_csv(article_filename)

# %%

























