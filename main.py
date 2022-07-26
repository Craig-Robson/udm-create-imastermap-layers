# coding: utf-8

import requests
import json
import geopandas as gpd
import pandas as pd
import zipfile, io
from os import listdir, getenv, mkdir, remove
import os
from os.path import isfile, join, isdir
import subprocess


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

# include developed excluding roads
run_developed_ex_roads = getenv('run_developed_ex_roads')
if run_developed_ex_roads.lower == 'false':
    run_developed_ex_roads = False
elif run_developed_ex_roads.lower == 'true':
    run_developed_ex_roads = True
else:
    run_developed_ex_roads = False

print('Run developed ex roads:', run_developed_ex_roads)

# get value of test flag
try:
    test = getenv('test')
except:
    test = False

if test.lower() == 'false':
    test = False
elif test.lower() == 'true':
    test = True
    print('Going to run in test mode')
else:
    test = False

print('TEST is:', test)

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
        zone_codes_lads.extend(list(gdf_zones['lad_code']))

if area_code_type.lower() == 'gor':
    area_code_list = zone_codes_lads
print(area_code_list)
for area_codes in area_code_list:
    print(area_codes)
    if area_codes == '':
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
    print(gdf_zones.columns)
    zone_codes = list(gdf_zones['msoa_code'])


    ## fetch topo polygons from NISMOD-DB
    scale = 'msoa'
    j = 0

    for zone_code in zone_codes:
        print(zone_code)
        queryText = f"https://www.nismod.ac.uk/api/data/mastermap/areas?export_format=geojson-zip&geom_format=geojson&scale={scale}&area_codes={zone_code}&year=2017&classification_codes=all&make=Manmade&flatten_lists=true"
        response = requests.get(queryText, auth=(user_settings['user'], user_settings['password']), verify=False)
        print(response.status_code)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(join(out_dir, area_codes))

        queryText = f"https://www.nismod.ac.uk/api/data/mastermap/areas?export_format=geojson-zip&geom_format=geojson&scale={scale}&area_codes={zone_code}&year=2017&classification_codes=all&make=Multiple&flatten_lists=true"
        response = requests.get(queryText, auth=(user_settings['user'], user_settings['password']), verify=False)
        print(response.status_code)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(join(out_dir, area_codes))

        # if in test mode stop here
        j += 1
        if test and j == 2: break

    # get list of files downloaded from API
    data_files = [f for f in listdir(join(out_dir, area_codes)) if isfile(join(out_dir, area_codes, f))]
    print('Downloaded files:', len(data_files))

    # for testing only
    #gdf = gpd.read_file(join(out_dir, area_codes, data_files[1]))
    #gdf.head()

    # loop through all downloaded geojson files and create a geodataframe
    path = [os.path.join(out_dir, area_codes, i) for i in data_files if ".geojson" in i]
    gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(i) for i in path],
                                     ignore_index=True), crs=gpd.read_file(path[0]).crs)

    # drop duplicate polygons and filter for manmade/developed surfaces
    gdf = gdf.drop_duplicates()
    gdf = gdf.loc[gdf['make'].isin(['Manmade', 'Multiple'])]

    # generate layer of urban/developed surfaces
    gdf.to_file('/data/outputs/developed/%s.gpkg' % area_codes, driver='GPKG')

    # generate layer excluding roads
    if run_developed_ex_roads is True:
        gdf_nr = gdf[~gdf.theme.str.contains('Roads Tracks And Paths')]
        gdf_nr.to_file('/data/outputs/developed_exroads/%s.gpkg' % area_codes, driver='GPKG')

    # if in test mode, stop here
    if test: break

print('Run merge process')

# check files available to merge
data_files = [f for f in listdir('/data/outputs/developed') if isfile(join('/data/outputs/developed', f))]
print('Downloaded files:', len(data_files))
print(data_files)

# create developed areas gpkg
command = "ogrmerge.py -single -o /data/outputs/developed/*.gpkg -f GPKG -o /data/outputs/final/developed.gpkg -overwrite_ds -progress"
subprocess.call(command, shell=True)

# create developed areas excluding roads
if run_developed_ex_roads is True:
    print('Running merge process for excluding roads layer')
    command = "ogrmerge.py -single -o /data/outputs/developed_exroads/*.gpkg -f GPKG -o /data/outputs/final/developed_exroads.gpkg -overwrite_ds -progress"
    subprocess.call(command, shell=True)
