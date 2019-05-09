import requests
import json
import gzip
import io
from bs4 import BeautifulSoup
import pandas as pd

# Birender's code is based off of this link as a reference
# https://www.devvid.com/using-python-and-common-crawl-to-find-products-from-amazon-com/

# %%
def search_domain(domain):
    record_list = []
    print("[*] Trying target domain: {}".format(domain)) 
    for index in index_list:
        print("[*] Trying index {}".format(index))
        cc_url = "http://index.commoncrawl.org/CC-MAIN-"
        cc_url += index
        cc_url += "-index?" 
        cc_url += "url="
        cc_url += domain
        cc_url += "&matchType=domain&output=json"
        # Make request to CommonCrawl index
        response = requests.get(cc_url)
        if response.status_code == 200:
            records = response.content.splitlines()
            for record in records:
                record_list.append(json.loads(record))  
            print("[*] Added {}results.".format(len(records)))
    print("[*] Found a total of {} hits.".format(len(record_list))) 
    return record_list
          
# %%
def download_page(record):
    
    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
    raw_data = io.StringIO(resp.content)
    f = gzip.GzipFile(fileobj=raw_data)
    data = f.read()
#     print(data)
    response = ""
    if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
#             print(warc)
        except:
            pass

    return response


# %%
def save_rawfilesnames(domain,record_list,filepath):
    cc_dict = {'filename':[],'offset':[],'length':[]}
    for record in record_list:
        cc_dict['filename'] = cc_dict['filename'] + [record['filename']]
        cc_dict['offset'] = cc_dict['offset'] + [record['offset']]
        cc_dict['length'] = cc_dict['length'] + [record['length']]
        
    cc_df = pd.DataFrame(cc_dict)
    cc_df.to_csv('data/cc/march2019/'+filepath+'Filenames.csv')

# %%
def download_pages(domain,record_list,filepath):
    i = 0
    art_dict = {'text':[]}
    for record in record_list:
        if i:
            print(i)
        i = i + 1
        offset, length = int(record['offset']), int(record['length'])
        offset_end = offset + length - 1
        prefix = 'https://commoncrawl.s3.amazonaws.com/'
        
        # Make request to specific common crawl archive based on current record
        resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
        # Read bytes to data NOTE THIS IS FOR PYTHON 3
        raw_data = io.BytesIO(resp.content)
        f = gzip.GzipFile(fileobj=raw_data)
        data = f.read()
        # print(data)
        response = ""
        if len(data):
            try:
                # Split the data into warc, header, and response
                warc, header, response = data.strip().split(b'\r\n\r\n', 2)
            except:
                pass
        parser = BeautifulSoup(response, "html.parser")
        # Depends on the HTML conventions followed by the domain
        # Add all paragraphs or otherwise content to the art_dict
        if filepath in ['trains']:
            li = parser.findAll("div", class_="content")
            for l in li:
                paragraphs = l.findChildren("div" , recursive=False)
                for p in paragraphs:
                    art_dict['text'] = art_dict['text'] + [p.text]        
        paragraphs = parser.findAll('p')
        for p in paragraphs:
            art_dict['text'] = art_dict['text'] + [p.text]
    
        if i == 700:
          break
      
    art_df = pd.DataFrame(art_dict)
    art_df.to_csv('data/cc/march2019/'+filepath+'.csv')

# %%
# NOTE: IF THIS IS CHANGED, RELOAD THE ABOVE FUNCTIONS
index_list = ["2019-13"]
# Query on these domains, roughly equivalent to keywords ['train','bike','bus','lightrail','car']
domains = ['http://trn.trains.com/','https://www.bicycling.com/','https://www.citylab.com/','https://www.amny.com/transit','https://jalopnik.com/c/news']

# For file io handling
filepaths = ['trains','bicycling','citylab','amnytransit','jalopnik']
filepathcounter = 0
# Loop through each filepath
for domain in filepaths:
    filepath = filepaths[filepathcounter]
    record_list = search_domain(domain)
    save_rawfilesnames(domain,record_list,filepath)
    download_pages(domain,record_list,filepath)
    filepathcounter = filepathcounter + 1
    
# %%
    
    