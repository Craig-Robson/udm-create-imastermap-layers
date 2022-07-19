# coding: utf-8

import requests
import json
import geopandas as gpd
import pandas as pd
import zipfile, io
from os import listdir, getenv, mkdir, remove
import os
from os.path import isfile, join, isdir


def fetch_settings():
    """Get the input environment variables"""
    api_user = getenv('api_user')
    api_password = getenv('api_password')
    #print(api_user, api_password)
    return api_user, api_password

# API credentials
user_settings = {}
api_username, api_password = fetch_settings()
user_settings['user'] = api_username
user_settings['password'] = api_password

# get area code scale (lad/gor)
area_code_type = getenv('area_code_type')
if area_code_type.lower() not in ['lad','gor']:
    print('bad area code type passed')
    exit()

# get list of area codes to run for - must be LAD codes
area_code_list = getenv('area_codes')
area_code_list = area_code_list.split(';')

def mk_dir(path):
    """"""
    if isdir(path) is False:
        mkdir(path)

def mk_dir_delete(path):
    """"""
    if isdir(path) is True:
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for file in files:
            remove(join(path, file))
    else:
        mkdir(path)

    return

mk_dir('/data')
mk_dir_delete('/data/downloads')
mk_dir_delete('/data/outputs')
mk_dir_delete('/data/outputs/developed')
mk_dir_delete('/data/outputs/developed_exroads')
mk_dir_delete('/data/outputs/final')
out_dir = '/data/downloads'


for area_codes in area_code_list:
    if area_codes == '':
        break
    print(area_codes)
    zone_codes_lads = []

    if area_code_type.lower() == 'gor':
        queryText = f"https://www.nismod.ac.uk/api/data/boundaries/lads_in_gor?export_format=geojson&gor_codes={area_codes}"
        print(queryText)
        response = requests.get(queryText, auth=(user_settings['user'], user_settings['password']), verify=False)
        if response.status_code != 200:
            print('API call for fetch LAD list failed! Response: ', response.status_code)
        area_zones = json.loads(response.text)
        gdf_zones = gpd.GeoDataFrame.from_features(area_zones["features"])
        zone_codes_lads.append(list(gdf_zones['lad_codes']))

if area_code_type.lower() == 'gor':
    area_codes_list = zone_codes_lads


for area_codes in area_codes_list:
    if area_codes = '':
        break

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
        print(zone_code)
        queryText = f"https://www.nismod.ac.uk/api/data/mastermap/areas?export_format=geojson-zip&geom_format=geojson&scale={scale}&area_codes={zone_code}&year=2017&classification_codes=all&make=Manmade&flatten_lists=true"
        response = requests.get(queryText, auth=(user_settings['user'], user_settings['password']), verify=False)
        print(response.status_code)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(join(out_dir, area_codes[:-1]))


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
    gdf.to_file('/data/outputs/developed/%s.gpkg' % area_codes, driver='GPKG')

    # generate layer excluding toads
    gdf_nr = gdf[~gdf.theme.str.contains('Roads Tracks And Paths')]
    gdf_nr.to_file('/data/outputs/developed_exroads/%s.gpkg' % area_codes, driver='GPKG')

# loop through all the generated files and create a geodataframe
data_files = [f for f in listdir(join('/data/outputs/developed_exroads')) if isfile(join('/data/outputs/developed_exroads', f))]
print('Data files:', data_files)
path = [os.path.join('/data/outputs/developed_exroads', i) for i in data_files if ".gpkg" in i]
gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(i) for i in path],
                                 ignore_index=True), crs=gpd.read_file(path[0]).crs)
gdf.to_file('/data/outputs/final/developed_exroads.gpkg', driver='GPKG')

# loop through all the generated files and create a geodataframe
data_files = [f for f in listdir(join('/data/outputs/developed')) if isfile(join('/data/outputs/developed', f))]
print('Data files:', data_files)
path = [os.path.join('/data/outputs/developed', i) for i in data_files if ".gpkg" in i]
gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(i) for i in path],
                                 ignore_index=True), crs=gpd.read_file(path[0]).crs)
gdf.to_file('/data/outputs/final/developed.gpkg', driver='GPKG')
