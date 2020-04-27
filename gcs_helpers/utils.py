import json
import secrets
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


def write_json(obj,path):
    with open(path,'w') as file:
        jsn=json.dump(obj,file)


def write_blob(blob,path,mode='w'):
    with open(path, mode) as file:
        blob.download_to_file(file)


#
# IMAGE HELPERS
#
def image_profile(lon,lat,crs,resolution,im,driver=GTIFF_DRIVER):
    count,height,width=im.shape
    x,y=transform(Proj(init='epsg:4326'),Proj(init=crs),lon,lat)
    x,y=int(round(x)),int(round(y))
    xmin=x-int(width/2)
    ymin=y-int(height/2)
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
def generate_name(tmp_name,ext=None):
    if not tmp_name:
        tmp_name=secrets.token_urlsafe(16)
    if ext and (not re.search(f'.{ext}$',tmp_name))
        tmp_name=f'{tmp_name}.{ext}'
    return tmp_name
