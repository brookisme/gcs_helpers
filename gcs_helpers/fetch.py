import os
from io import BytesIO
import re
import secrets
from google.cloud import storage
import pandas as pd
import rasterio as rio
from . import utils


GS_PREFIX='gs://'


def bucket_key_from_path(path):
    path=re.sub('^gs://','',path)
    parts=path.split('/')
    bucket=parts[0]
    key="/".join(parts[1:])
    return bucket, key


def blob(
        bucket=None,
        key=None,
        dest=None,
        dest_folder=None,
        ext=None,
        as_data=False,
        path=None,
        write_mode='wb',
        project=None,
        client=None):
    if not client:
        client=storage.Client(project=project)
    if path:
        bucket, key=bucket_key_from_path(path)
    bucket=client.get_bucket(bucket)
    blob=bucket.blob(key)
    if as_data:
        data = BytesIO()
        blob.download_to_file(data)
        data.seek(0)
        return data
    else:
        if not dest:
            dest=utils.generate_name(
                name=dest,
                ext=ext,
                folder=dest_folder)
        utils.write_blob(blob,dest,mode=write_mode)
        return dest


def image(
        bucket=None,
        key=None,
        dest=None,
        dest_folder=None,
        ext='tif',
        path=None,
        write_mode='wb',
        project=None,
        return_data=True,
        remove_data=True,
        return_dest_with_data=False,
        return_profile=True,
        client=None):
    dest=blob(
        bucket=bucket,
        key=key,
        dest=dest,
        dest_folder=dest_folder,
        ext=ext,
        path=path,
        write_mode=write_mode,
        project=project,
        client=client)
    if return_data:
        data=_read_image(dest,return_profile=return_profile)
        if remove_data:
            os.remove(dest)
        if (not remove_data) and return_dest_with_data:
            return data, dest
        else:
            return data
    else:
        return dest


def csv(
        bucket=None,
        key=None,
        dest=None,
        dest_folder=None,
        ext='csv',
        path=None,
        write_mode='wb',
        project=None,
        return_data=True,
        remove_data=True,
        return_dest_with_data=False,
        return_profile=True,
        client=None,
        **read_csv_kwargs):
    dest=blob(
        bucket=bucket,
        key=key,
        dest=dest,
        dest_folder=dest_folder,
        ext=ext,
        path=path,
        write_mode=write_mode,
        project=project,
        client=client)
    if return_data:
        data=pd.read_csv(dest,**read_csv_kwargs)
        if remove_data:
            os.remove(dest)
        if (not remove_data) and return_dest_with_data:
            return data, dest
        else:
            return data
    else:
        return dest


#
# INTERNAL
#
def _read_image(path,return_profile=True,dtype=None):
    with rio.open(path,'r') as src:
        if return_profile:
            profile=src.profile
        image=src.read()
        if dtype:
            image=image.astype(dtype)
    if return_profile:
        return image, profile
    else:
        return image


