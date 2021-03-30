import os
import pandas
from pandas import read_csv
import json
import pickle
from datetime import datetime
import sys
import requests

#### Create curatedBy Object
def generate_curator():
    todate = datetime.now()
    curatedByObject = {"@type": "Organization", "identifier": "covid19LST", "url": "https://www.covid19lst.org/", 
                              "name": "COVID-19 Literature Surveillance Team", "affiliation": "", 
                              "curationDate": todate.strftime("%Y-%m-%d")}
    return(curatedByObject)

def check_google():
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive
    from pydrive2.auth import ServiceAccountCredentials
    
    gauth = GoogleAuth()
    scope = ['https://www.googleapis.com/auth/drive']
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    drive = GoogleDrive(gauth)
    file_id = '1vC8oXhfhogAh7olq9BvEPdwwvyeXsZkk'
    file_list = drive.ListFile({'q': "'%s' in parents and trashed=false" % file_id}).GetList()
    
    df = pandas.DataFrame(file_list)
    dfclean = df[['createdDate','id','title']].copy()
    dfclean['date'] = pandas.to_datetime(dfclean['createdDate'],format='%Y-%m-%d', errors='coerce')
    lastupdate = dfclean.loc[dfclean['createdDate']=='2020-09-28T22:28:33.989Z'].iloc[0]['date']
    dfnew = dfclean.loc[dfclean['date']>lastupdate]

    all_files = os.listdir('data/tables/')
    new_files = [item for item in dfnew['title'].unique().tolist() if item not in all_files]
    tabledf = dfnew.loc[dfnew['title'].isin(new_files)]
    return(tabledf)

## This is the function to actually conduct the download
def download_dumps(dumpdf):
    from google_drive_downloader import GoogleDriveDownloader as gdd
    for i in range(len(dumpdf)):
        title = dumpdf.iloc[i]['title']
        eachid = dumpdf.iloc[i]['id']
        gdd.download_file_from_google_drive(file_id=eachid,
                                            dest_path='data/tables/'+title,
                                            unzip=False)  

def fetch_src_size(source):
    pubmeta = requests.get("https://api.outbreak.info/resources/query?q=curatedBy.identifier:"+source+"&size=0&aggs=@type")
    pubjson = json.loads(pubmeta.text)
    pubcount = int(pubjson["facets"]["@type"]["total"])
    return(pubcount)

def get_ids_from_json(jsonfile):
    idlist = []
    for eachhit in jsonfile["hits"]:
        if eachhit["_id"] not in idlist:
            idlist.append(eachhit["_id"])
    return(idlist)

def get_source_ids(source):
    source_size = fetch_src_size(source)
    r = requests.get("https://api.outbreak.info/resources/query?q=curatedBy.identifier:"+source+"&fields=_id&fetch_all=true")
    response = json.loads(r.text)
    idlist = get_ids_from_json(response)
    try:
        scroll_id = response["_scroll_id"]
        while len(idlist) < source_size:
            r2 = requests.get("https://api.outbreak.info/resources/query?q=curatedBy.identifier:"+source+"&fields=_id&fetch_all=true&scroll_id="+scroll_id)
            response2 = json.loads(r2.text)
            idlist2 = set(get_ids_from_json(response2))
            tmpset = set(idlist)
            idlist = tmpset.union(idlist2)
            try:
                scroll_id = response2["_scroll_id"]
            except:
                print("no new scroll id")
        return(idlist)
    except:
        return(idlist)
    

def batch_fetch_meta(idlist):
    ## Break the list of ids into smaller chunks so the API doesn't fail the post request
    runs = round((len(idlist))/100,0)
    i=0 
    separator = ','
    rawdf = pandas.DataFrame(columns = ['_id','name','url','isBasedOn'])
    while i < runs+1:
        if len(idlist)<100:
            sample = idlist
        elif i == 0:
            sample = idlist[i:(i+1)*100]
        elif i == runs:
            sample = idlist[i*100:len(idlist)]
        else:
            sample = idlist[i*100:(i+1)*100]
        sample_ids = separator.join(sample)
        r = requests.post("https://api.outbreak.info/resources/query/", params = {'q': sample_ids, 'scopes': '_id', 'fields': 'name,url,isBasedOn'})
        if r.status_code == 200:
            rawresult = pandas.read_json(r.text)
            cleanresult = rawresult[['_id','name','url','isBasedOn']].loc[rawresult['_score']==1].copy()
            cleanresult.drop_duplicates(subset='_id',keep="first", inplace=True)
            rawdf = pandas.concat((rawdf,cleanresult))
        i=i+1
    return(rawdf)


def generate_citedby_df(rawdf):
    citedkeys = ['_id','name','url']
    zipped = zip(rawdf['_id'], rawdf['name'], rawdf['url'])
    rawdf['citedBy'] = [dict(zip(citedkeys, values)) for values in zipped]
    rawdf.drop(columns=['_id','name','url'],inplace=True)
    explodedf = rawdf.explode('isBasedOn')
    tmpdf = pandas.json_normalize(explodedf['isBasedOn'])
    tmpzip = zip(tmpdf['_id'], tmpdf['name'], tmpdf['url'])
    tmpdf['isBasedOn'] = [dict(zip(citedkeys, values)) for values in tmpzip]
    tmpdf['isBasedOn'] = tmpdf['isBasedOn'].astype(str)
    explodedf['isBasedOn'] = explodedf['isBasedOn'].astype(str)
    cleandf = explodedf.merge(tmpdf, on='isBasedOn',how='inner')
    cleandf['citedstr']=cleandf['citedBy'].astype(str)
    cleandf.drop_duplicates(subset=['citedstr','_id'],keep="first",inplace=True)
    cleandf.drop(columns=['name','url','isBasedOn','citedstr'],inplace=True)
    return(cleandf)


def get_report_links():
    source = "covid19LST"
    idlist = get_source_ids(source)
    rawdf = batch_fetch_meta(idlist)
    cleandf = generate_citedby_df(rawdf)
    return(cleandf)
        
def update_filelist():
    all_files = os.listdir('data/tables/')
    updatefiles = all_files.remove('covid19LST_1st_dump.csv')
    initial_file = 'data/tables/covid19LST_1st_dump.csv'
    df = read_csv(initial_file,header=0,usecols=['PMID','Topics','LevelOfEvidence','Methodology','Updated Date'])
    if updatefiles!=None:
        for eachfile in updatefiles:
            tmpfile = read_csv('data/tables/'+eachfile,header=0,usecols=['PMID','Topics','LevelOfEvidence','Methodology','Updated Date'])
            df = pandas.concat((df,tmpfile),ignore_index=True)
    else:
        nochange=True
    df.sort_values('Updated Date',ascending=False,inplace=True)
    df.drop_duplicates(subset='PMID',keep='first',inplace=True)
    return(df)    

def fix_keywords(keywordstring):
    if keywordstring != keywordstring: ## Is it Nan?
        keywordlist = []
    elif keywordstring =="": ## Is it an empty string?
        keywordlist = []
    elif keywordstring == None: ## Is there no keywordstring?
        keywordlist = []
    else:
        keywordlist = keywordstring.lstrip('[').rstrip(']').replace('"','').split(',')
    return(keywordlist)

def generate_dump(datadmp):
    cleandata = []
    authorObject = generate_curator()
    datadmp['PMID'] = datadmp['PMID'].astype(int) 
    cleandf = get_report_links()
    cleandf['PMID'] = cleandf['_id'].str.replace('pmid','').astype(int)
    mergedmp = datadmp.merge(cleandf, on='PMID', how='inner').fillna('-1')
    for i in range(len(mergedmp)):
        keywordlist = fix_keywords(mergedmp.iloc[i]['Topics'])
        tmpdict={'_id':mergedmp.iloc[i]['_id'],'keywords':keywordlist,
                 'evaluations':[{'@type':'Rating',
                                'name':'covid19LST',
                                'ratingExplanation':mergedmp.iloc[i]['Methodology'],
                                'ratingValue':mergedmp.iloc[i]['LevelOfEvidence'],
                                'reviewAspect':'Oxford 2011 Levels of Evidence',
                                'author':authorObject}]}        
        if mergedmp.iloc[i]['citedBy']!='-1':
            tmpdict['citedBy']=mergedmp.iloc[i]['citedBy']            
        cleandata.append(tmpdict)
    return(cleandata)

def run_loe_update():
    dumpdf = check_google()
    download_dumps(dumpdf)
    datadmp = update_filelist()
    dictlist = generate_dump(datadmp)
    for eachdict in dictlist:
        yield(eachdict)
       
 