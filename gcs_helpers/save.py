import os
import secrets
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from retrying import retry
import rasterio as rio
from pyproj import Proj, transform
from affine import Affine
from rasterio.crs import CRS
import .utils as utils
#
# CONSTANTS
#
TIF_MIME_TYPE='image/tiff'
PNG_MIME_TYPE='image/png'
CSV_MIME_TYPE='text/csv'
JSON_MIME_TYPE='application/json'
GEOJSON_MIME_TYPE='application/geo+json'
GTIFF_DRIVER='GTiff'
PNG_DRIVER='PNG'



#
# CONFIG
#
WAIT_EXP_MULTIPLIER=1000
WAIT_EXP_MAX=1000
STOP_MAX_ATTEMPT=7
TMP_NAME=None
DEFAULT_IMAGE_MIME_TYPE=TIF_MIME_TYPE
DEFAULT_MIME_TYPE=JSON_MIME_TYPE



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
# GOOGLE STORAGE HELPERS
#
def gcs_service(service=None):
    """ get gcloud storage client if it does not already exist """
    if not service:
        service=build('storage', 'v1')
    return service


@retry(
    wait_exponential_multiplier=WAIT_EXP_MULTIPLIER, 
    wait_exponential_max=WAIT_EXP_MAX,
    stop_max_attempt_number=STOP_MAX_ATTEMPT)
def save_to_gcs(
        src,
        dest=None,
        mtype=DEFAULT_MIME_TYPE,
        folder=None,
        bucket=None,
        service=None,
        return_path=True):
    """ save file to google cloud storage
        * src<str>: source path
        * dest<str>: 
            - file path on google cloud storage
            - if bucket not passed bucket with be assumed to be the first 
              part of the path
        * mtype<str>: mime type
        * folder<str>: prefixed to dest path above
        * bucket<str>: 
            - gcloud bucket
            - if bucket not passed bucket with be assumed to be the first 
              part of the dest path
        * service<google-storage-client|None>: if none, create client
        * return_path<bool>: 
            - if true return gc://{bucket}/{path}
            - else return response from request  
    """
    if not dest:
        dest=os.path.basename(src)
    media = MediaFileUpload(
        src, 
        mimetype=mtype,
        resumable=True)
    path, bucket=_gcs_path_and_bucket(dest,folder,bucket)
    request=gcs_service(service).objects().insert(
                                    bucket=bucket, 
                                    name=path,
                                    media_body=media)
    response=None
    while response is None:
        _, response=request.next_chunk()
    if return_path:
        return f'gs://{bucket}/{path}'
    else:
        return response


def image_to_gcs(
    im,
    dest,
    profile=None,
    mtype=DEFAULT_IMAGE_MIME_TYPE,
    png=False,
    tmp_name=TMP_NAME,
    folder=None,
    bucket=None,
    service=None,
    delete_src_file=False,
    save_tmp_file=False,
    return_path=True):
    """
    """
    if png:
        mtype=PNG_MIME_TYPE
        ext='png'
    else:
        mtype=mtype
        ext='tif'
    if not isinstance(im,str):
        tmp_name=_get_tmp_name(tmp_name,ext)
        with rio.open(tmp_name,'w',**profile) as dst:
                dst.write(im)
        im=tmp_name
    return _save_and_clean(
        src=im,
        dest=dest,
        mtype=mtype,
        folder=folder,
        bucket=bucket,
        service=service,
        return_path=return_path,
        delete_src_file=delete_src_file)


def csv_to_gcs(
    dataset,
    dest,
    tmp_name=TMP_NAME,
    folder=None,
    bucket=None,
    service=None,
    delete_src_file=False,
    save_tmp_file=False,
    return_path=True):
    """
    """  
    if not isinstance(dataset,str):
        tmp_name=_get_tmp_name(tmp_name,'csv')
        dataset.to_csv(tmp_name,index=False)
        dataset=tmp_name
    return _save_and_clean(
        src=dataset,
        dest=dest,
        mtype=CSV_MIME_TYPE,
        folder=folder,
        bucket=bucket,
        service=service,
        return_path=return_path,
        delete_src_file=delete_src_file)


def json_to_gcs(
    dataset,
    dest,
    tmp_name=TMP_NAME,
    geojson=False,
    folder=None,
    bucket=None,
    service=None,
    delete_src_file=False,
    save_tmp_file=False,
    return_path=True):
    """
    """  
    if geojson:
        mtype=GEOJSON_MIME_TYPE
        ext='geojson'
    else:
        mtype=JSON_MIME_TYPE
        ext='json'
    if isinstance(dataset,str):
        tmp_name=dataset
    else:
        tmp_name=_get_tmp_name(tmp_name,ext)
        utils.write(dataset,tmp_name)
        dataset=tmp_name
        delete_src_file=(not save_tmp_file)
    return _save_and_clean(
        src=dataset,
        dest=dest,
        mtype=mtype,
        folder=folder,
        bucket=bucket,
        service=service,
        return_path=return_path,
        delete_src_file=delete_src_file)


#
# INTERNAL
#
def _get_tmp_name(tmp_name,ext=None):
    if not tmp_name:
        tmp_name=secrets.token_urlsafe(16)
    if ext:
        tmp_name=f'{tmp_name}.{ext}'
    return tmp_name


def _gcs_path_and_bucket(dest,folder,bucket):
    if not bucket:
        parts=dest.split('/')
        bucket=parts[0]
        dest='/'.join(parts[1:])
    if folder:
        dest='{}/{}'.format(folder,dest)
    return dest, bucket


def _save_and_clean(src,dest,mtype,folder,bucket,service,return_path,delete_src_file):
    out=save_to_gcs(
        src=src,
        dest=dest,
        mtype=mtype,
        folder=folder,
        bucket=bucket,
        service=service,
        return_path=return_path)
    if delete_src_file:
        os.remove(src)
    return out
