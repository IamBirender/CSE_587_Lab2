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
keywords = ['sample']

# %%
# Twitter
# Download word count data from S3 bucket and save raw output to local files
for k in keywords:
    existing_df = pd.DataFrame()
    response = client.list_objects(Bucket='mpbrown-ubcse487', Prefix='output/wordcount/tw/'+k)
    
    if 'Contents' in response:
        for r in response['Contents'][1:]:
           obj = client.get_object(Bucket='mpbrown-ubcse487', Key=r['Key'])
           body_df = pd.read_csv(obj.get('Body'), sep='\t', names=['Word','Count'])
           existing_df = pd.concat([existing_df, body_df], ignore_index=True, names=['Word','Count'])
           
        existing_df.to_csv('output/wordcount/tw/'+k+'_count.csv')
    
# Process and filter data into the topten words
stop_words = set(stopwords.words('english'))

for k in keywords:
    filename = 'output/wordcount/tw/'+k+'_count.csv'
    if os.path.isfile(filename):
        count_df = pd.read_csv(filename, index_col=0)
        topten = count_df[~count_df['Word'].isin(stop_words)].nlargest(10,'Count')
        topten.to_csv('output/wordcount/tw/'+k+'_topten.csv')
        
# %%
# New York Times
# Download word count data from S3 bucket and save raw output to local files
for k in keywords:
    existing_df = pd.DataFrame()
    response = client.list_objects(Bucket='mpbrown-ubcse487', Prefix='output/wordcount/nyt/'+k)
    
    if 'Contents' in response:
        for r in response['Contents'][1:]:
           obj = client.get_object(Bucket='mpbrown-ubcse487', Key=r['Key'])
           body_df = pd.read_csv(obj.get('Body'), sep='\t', names=['Word','Count'])
           existing_df = pd.concat([existing_df, body_df], ignore_index=True, names=['Word','Count'])
           
        existing_df.to_csv('output/wordcount/nyt/'+k+'_count.csv')
    
# Process and filter data into the topten words
stop_words = set(stopwords.words('english'))

for k in keywords:
    filename = 'output/wordcount/nyt/'+k+'_count.csv'
    if os.path.isfile(filename):
        count_df = pd.read_csv(filename, index_col=0)
        topten = count_df[~count_df['Word'].isin(stop_words)].nlargest(10,'Count')
        topten.to_csv('output/wordcount/nyt/'+k+'_topten.csv')
        
# %%
# Common Crawl
# Download word count data from S3 bucket and save raw output to local files
domainpaths = ['trains','bicycling','citylab','transit','jalopnik']
for k in domainpaths:
    existing_df = pd.DataFrame()
    response = client.list_objects(Bucket='mpbrown-ubcse487', Prefix='output/wordcount/cc/'+k)
    
    if 'Contents' in response:
        for r in response['Contents'][1:]:
           obj = client.get_object(Bucket='mpbrown-ubcse487', Key=r['Key'])
           body_df = pd.read_csv(obj.get('Body'), sep='\t', names=['word','count'])
           existing_df = pd.concat([existing_df, body_df], ignore_index=True, names=['word','word'])
           
        existing_df.to_csv('output/wordcount/cc/'+k+'_count.csv')

# Process and filter data into the topten words
stop_words = set(stopwords.words('english'))

for k in domainpaths:
    filename = 'output/wordcount/cc/'+k+'_count.csv'
    if os.path.isfile(filename):
        count_df = pd.read_csv(filename, index_col=0)
        topten = count_df[~count_df['word'].isin(stop_words)].nlargest(10,'count')
        topten.to_csv('output/wordcount/cc/'+k+'_topten.csv')
        
# %%














