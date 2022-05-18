#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import geopandas as gpd
import pandas as pd
import zipfile, io
from os import listdir, getenv, mkdir
import os
from os.path import isfile, join

def fetch_settings():
    """Get the input environment variables"""
    api_user = getenv('api_user')
    api_password = getenv('api_password')
    print(api_user, api_password)
    return api_user, api_password

# API credentials
user_settings = {}
api_username, api_password = fetch_settings()
user_settings['user'] = api_username
user_settings['password'] = api_password

mkdir('/data')
mkdir('/data/downloads')
mkdir('/data/outputs')
mkdir('/data/outputs/developed')
mkdir('/data/outputs/developed_exroads')
out_dir = '/data/downloads'

area_codes = 'W06000012,'
queryText = f"https://www.nismod.ac.uk/api/data/boundaries/msoas_in_lad?export_format=geojson&area_codes={area_codes}"
print(queryText)
response = requests.get(queryText, auth=(user_settings['user'], user_settings['password']), verify=False)
if response.status_code != 200:
    print('API call for fetch MSOA list failed! Response: ', response.status_code)
print(response.status_code)
# load msoa/zone data into geodataframe
area_zones = json.loads(response.text)
gdf_zones = gpd.GeoDataFrame.from_features(area_zones["features"])

zone_codes = list(gdf_zones['msoa_code'])


## fetch topo polygons from NISMOD-DB
scale = 'msoa'

for zone_code in zone_codes:
    queryText = f"https://www.nismod.ac.uk/api/data/mastermap/areas?export_format=geojson-zip&geom_format=geojson&scale={scale}&area_codes={zone_code}&year=2017&classification_codes=all&make=Manmade&flatten_lists=true"
    response = requests.get(queryText, auth=(user_settings['user'], user_settings['password']), verify=False)
    print(response.status_code)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall(join(out_dir, area_codes[:-1]))
    break


# get list of files downloaded from API
data_files = [f for f in listdir(join(out_dir, area_codes[:-1])) if isfile(join(out_dir, area_codes[:-1], f))]

# for testing only
#gdf = gpd.read_file(join(out_dir, area_codes[:-1], data_files[1]))
#gdf.head()

# loop through all downloaded geojson files and create a geodataframe
path = [os.path.join(out_dir, area_codes[:-1], i) for i in data_files if ".geojson" in i]
gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(i) for i in path],
                                 ignore_index=True), crs=gpd.read_file(path[0]).crs)

# drop duplicate polygons and filter for manmade/developed surfaces
gdf = gdf.drop_duplicates()
gdf = gdf.loc[gdf['make'].isin(['Manmade', 'Multiple'])]

# generate layer of urban/developed surfaces
gdf.to_file('data/outputs/developed/%s.shp' % area_codes[:-1])

# generate layer excluding toads
gdf_nr = gdf[~gdf.theme.str.contains('Roads Tracks And Paths')]
gdf_nr.to_file('data/outputs/developed_exroads/%s.shp' % area_codes[:-1])