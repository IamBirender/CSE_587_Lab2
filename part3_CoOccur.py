import boto3
import keyring
import os.path
import pandas as pd
from nltk.corpus import stopwords

# %%
# Set up boto3 authorization for accessing Amazon Web Services S3
client = boto3.client(
    's3',
    aws_access_key_id=keyring.get_password('aws_access_key', 'mpbrown15'),
    aws_secret_access_key=keyring.get_password('aws_secret_access_key', 'mpbrown15'),
)

keywords = ['bike', 'bus', 'car', 'pedestrian', 'lightrail','plane', 'train']

# %%
# Twitter
# Download word co-occurance data from S3 bucket and save raw output to local files
for k in keywords:
    existing_df = pd.DataFrame()
    response = client.list_objects(Bucket='mpbrown-ubcse487', Prefix='output/wordcooccur/tw/'+k)
    
    if 'Contents' in response:
        for r in response['Contents'][1:]:
           obj = client.get_object(Bucket='mpbrown-ubcse487', Key=r['Key'])
           body_df = pd.read_csv(obj.get('Body'), sep='\t', names=['pair','count'])
           existing_df = pd.concat([existing_df, body_df], ignore_index=True, names=['pair','count'])
           
        existing_df.to_csv('output/cooccur/tw/raw/'+k+'_count.csv')
        
        
# %%
# Twitter
# Process and clean raw data by converting the 'pair' string into two strings that are stored in 'word' and 'cooccurword' columns, respectively
for k in keywords:
    #print(k)
    tuple_df = pd.DataFrame()
    raw_df = pd.read_csv('output/cooccur/tw/raw/'+k+'_count.csv', index_col=0)
    wordseries = []
    cowordseries = []
    for pair in raw_df['pair']:
        wordHalf = pair.split(',')[0][1:]
        cowordHalf = pair.split(',')[1][:-1]
        #print('pre '+wordHalf+' , '+cowordHalf)
        alphaPair = sorted([wordHalf,cowordHalf])
        #print(str(alphaPair))
        wordseries = wordseries + [alphaPair[0]]
        cowordseries = cowordseries + [alphaPair[1]]
        
    raw_df['word'] = wordseries
    raw_df['cooccurword'] = cowordseries
    raw_df = raw_df.assign(alphaword = lambda x: x.word+x.cooccurword)
    raw_df = raw_df.drop_duplicates(subset=['alphaword'])
    raw_df.to_csv('output/cooccur/tw/'+k+'_count.csv')
        

# Process and filter data into the topten words
stop_words = set(stopwords.words('english'))

for k in keywords:
    filename = 'output/cooccur/tw/'+k+'_count.csv'
    if os.path.isfile(filename):
        count_df = pd.read_csv(filename, index_col=0)
        nodups = count_df[count_df.word != count_df.cooccurword]
        nodups = nodups[~nodups['word'].isin(stop_words)]
        nodups = nodups[~nodups['cooccurword'].isin(stop_words)]
        nodups = nodups.dropna()
        topten = nodups.nlargest(10,'count')
        topten.to_csv('output/cooccur/tw/'+k+'_topten.csv')
        
# %%
# New York Times
# Download word co-occurance data from S3 bucket and save raw output to local files
for k in keywords:
    existing_df = pd.DataFrame()
    response = client.list_objects(Bucket='mpbrown-ubcse487', Prefix='output/wordcooccur/nyt/'+k)
    
    if 'Contents' in response:
        for r in response['Contents'][1:]:
           obj = client.get_object(Bucket='mpbrown-ubcse487', Key=r['Key'])
           body_df = pd.read_csv(obj.get('Body'), sep='\t', names=['pair','count'])
           existing_df = pd.concat([existing_df, body_df], ignore_index=True, names=['pair','count'])
           
        existing_df.to_csv('output/cooccur/nyt/raw/'+k+'_count.csv')
    
# %%
# New York Times
# Process and clean raw data by converting the 'pair' string into two strings that are stored in 'word' and 'cooccurword' columns, respectively
for k in keywords:
    print(k)
    tuple_df = pd.DataFrame()
    raw_df = pd.read_csv('output/cooccur/nyt/raw/'+k+'_count.csv', index_col=0)
    wordseries = []
    cowordseries = []
    for pair in raw_df['pair']:
        wordHalf = pair.split(',')[0][1:]
        cowordHalf = pair.split(',')[1][:-1]
        #print('pre '+wordHalf+' , '+cowordHalf)
        alphaPair = sorted([wordHalf,cowordHalf])
        #print(str(alphaPair))
        wordseries = wordseries + [alphaPair[0]]
        cowordseries = cowordseries + [alphaPair[1]]
        
    raw_df['word'] = wordseries
    raw_df['cooccurword'] = cowordseries
    raw_df = raw_df.assign(alphaword = lambda x: x.word+x.cooccurword)
    raw_df = raw_df.drop_duplicates(subset=['alphaword'])
    raw_df.to_csv('output/cooccur/nyt/'+k+'_count.csv')


# Process and filter data into the topten words
stop_words = set(stopwords.words('english'))

for k in keywords:
    filename = 'output/cooccur/nyt/'+k+'_count.csv'
    if os.path.isfile(filename):
        count_df = pd.read_csv(filename, index_col=0)
        nodups = count_df[count_df.word != count_df.cooccurword]
        nodups = nodups[~nodups['word'].isin(stop_words)]
        nodups = nodups[~nodups['cooccurword'].isin(stop_words)]
        nodups = nodups.dropna()
        topten = nodups.nlargest(10,'count')
        topten.to_csv('output/cooccur/nyt/'+k+'_topten.csv')
        
# %%
# Common Crawl
# Download word co-occurance data from S3 bucket and save raw output to local files
domainpaths = ['trains','bicycling','citylab','transit','jalopnik']
for k in domainpaths:
    existing_df = pd.DataFrame()
    response = client.list_objects(Bucket='mpbrown-ubcse487', Prefix='output/wordcooccur/cc/'+k)
    
    if 'Contents' in response:
        for r in response['Contents'][1:]:
           obj = client.get_object(Bucket='mpbrown-ubcse487', Key=r['Key'])
           body_df = pd.read_csv(obj.get('Body'), sep='\t', names=['pair','count'])
           existing_df = pd.concat([existing_df, body_df], ignore_index=True, names=['pair','word'])
           
        existing_df.to_csv('output/cooccur/cc/raw/'+k+'_count.csv')        

# %%
# Common Crawl
# Process and clean raw data by converting the 'pair' string into two strings that are stored in 'word' and 'cooccurword' columns, respectively
domainpaths = ['trains','bicycling','citylab','transit','jalopnik']
for k in domainpaths: 
    if os.path.isfile('output/cooccur/cc/raw/'+k+'_count.csv'):
        print(k)
        tuple_df = pd.DataFrame()
        raw_df = pd.read_csv('output/cooccur/cc/raw/'+k+'_count.csv', index_col=0)
        wordseries = []
        cowordseries = []
        for pair in raw_df['pair']:
            wordHalf = pair.split(',')[0][1:]
            cowordHalf = pair.split(',')[1][:-1]
            #print('pre '+wordHalf+' , '+cowordHalf)
            alphaPair = sorted([wordHalf,cowordHalf])
            #print(str(alphaPair))
            wordseries = wordseries + [alphaPair[0]]
            cowordseries = cowordseries + [alphaPair[1]]
            
        raw_df['word'] = wordseries
        raw_df['cooccurword'] = cowordseries
        raw_df = raw_df.assign(alphaword = lambda x: x.word+x.cooccurword)
        raw_df = raw_df.drop_duplicates(subset=['alphaword'])
        raw_df.to_csv('output/cooccur/cc/'+k+'_count.csv')
        
# Process and filter data into the topten words
stop_words = set(stopwords.words('english'))

for k in domainpaths:
    filename = 'output/cooccur/cc/'+k+'_count.csv'
    if os.path.isfile(filename):
        count_df = pd.read_csv(filename, index_col=0)
        nodups = count_df[count_df.word != count_df.cooccurword]
        nodups = nodups[~nodups['word'].isin(stop_words)]
        nodups = nodups[~nodups['cooccurword'].isin(stop_words)]
        nodups = nodups.dropna()
        topten = nodups.nlargest(10,'count')
        topten.to_csv('output/cooccur/cc/'+k+'_topten.csv')
        
# %%














