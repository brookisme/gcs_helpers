import re
import json
import geojson
import secrets
from pathlib import Path
from affine import Affine
from rasterio.crs import CRS
from pyproj import Transformer
#
# CONSTANTS
#
GTIFF_DRIVER='GTiff'
PNG_DRIVER='PNG'


#
# I/O
#
def read_json(path,*key_path):
    with open(path,'r') as file:
        jsn=json.load(file)
    for k in key_path:
        jsn=jsn[k]
    return jsn


def read_geojson(path,*key_path):
    with open(path,'r') as file:
        jsn=geojson.load(file)
    for k in key_path:
        jsn=jsn[k]
    return jsn


def write_json(obj,path):
    with open(path,'w') as file:
        jsn=json.dump(obj,file)


def write_blob(blob,path,mode='w',check_existence=True):
    if (not check_existence) or blob.exists():
        with open(path, mode) as file:
            blob.download_to_file(file)


#
# IMAGE HELPERS
#
def image_profile(lon,lat,crs,resolution,im,driver=GTIFF_DRIVER):
    count,height,width=im.shape
    x,y=Transformer.from_crs("epsg:4326",crs).transform(lat,lon)
    xmin=round(x-(width*resolution/2))
    ymin=round(y-(height*resolution/2))
    profile={
        'count': count,
        'crs': CRS.from_string(crs),
        'driver': GTIFF_DRIVER,
        'dtype': im.dtype,
        'height': height,
        'nodata': None,
        'transform': Affine(resolution,0,xmin,0,-resolution,ymin),
        'width': width }
    if driver==GTIFF_DRIVER:
        profile.update({
            'compress': 'lzw',
            'interleave': 'pixel',
            'tiled': False
        })
    return profile


#
# SHARED
#
def generate_name(name=None,ext=None,folder=None,create_folder=True):
    if not name:
        name=secrets.token_urlsafe(16)
    if ext and (not re.search(f'.{ext}$',name)):
        name=f'{name}.{ext}'
    if folder:
        name=f'{folder}/{name}'
        if create_folder:
            Path(name).parent.mkdir(parents=True, exist_ok=True)
    return name
