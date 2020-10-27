import os
import re
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from retrying import retry
import rasterio as rio
from pyproj import Proj, transform
from affine import Affine
from rasterio.crs import CRS
from . import utils
#
# CONSTANTS
#
TIF_MIME_TYPE='image/tiff'
PNG_MIME_TYPE='image/png'
CSV_MIME_TYPE='text/csv'
JSON_MIME_TYPE='application/json'
GEOJSON_MIME_TYPE='application/geo+json'
PNG_EXT='png'
TIF_EXT='tif'


#
# CONFIG
#
WAIT_EXP_MULTIPLIER=1000
WAIT_EXP_MAX=1000
STOP_MAX_ATTEMPT=7
TMP_NAME=None
DEFAULT_IMAGE_MIME_TYPE=TIF_MIME_TYPE
DEFAULT_MIME_TYPE=JSON_MIME_TYPE
DEFAULT_EXT=TIF_EXT


#
# MAIN
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


def to_gcs(
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


def image(
    im,
    dest,
    profile=None,
    mtype=DEFAULT_IMAGE_MIME_TYPE,
    ext=DEFAULT_EXT,
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
        ext=PNG_EXT
    if not isinstance(im,str):
        tmp_name=utils.generate_name(tmp_name,ext)
        with rio.open(tmp_name,'w',**profile) as dst:
                dst.write(im)
        im=tmp_name
        delete_src_file=(not save_tmp_file)
    return _save_and_clean(
        src=im,
        dest=dest,
        mtype=mtype,
        folder=folder,
        bucket=bucket,
        service=service,
        return_path=return_path,
        delete_src_file=delete_src_file)


def csv(
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
        tmp_name=utils.generate_name(tmp_name,'csv')
        dataset.to_csv(tmp_name,index=False)
        dataset=tmp_name
        delete_src_file=(not save_tmp_file)
    return _save_and_clean(
        src=dataset,
        dest=dest,
        mtype=CSV_MIME_TYPE,
        folder=folder,
        bucket=bucket,
        service=service,
        return_path=return_path,
        delete_src_file=delete_src_file)


def json(
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
        tmp_name=utils.generate_name(tmp_name,ext)
        utils.write_json(dataset,tmp_name)
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
def _gcs_path_and_bucket(dest,folder,bucket):
    dest=re.sub('^gs://','',dest)
    if not bucket:
        parts=dest.split('/')
        bucket=parts[0]
        dest='/'.join(parts[1:])
    if folder:
        dest='{}/{}'.format(folder,dest)
    return dest, bucket


def _save_and_clean(src,dest,mtype,folder,bucket,service,return_path,delete_src_file):
    out=to_gcs(
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
