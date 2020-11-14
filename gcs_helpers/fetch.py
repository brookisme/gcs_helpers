import os
from io import BytesIO
import re
import secrets
from google.cloud import storage
import pandas as pd
from affine import Affine
import rasterio as rio
from rasterio.enums import Resampling
from rasterio.windows import Window
from . import utils


FIRST='first'
LAST='last'
BAND_ORDERING=os.environ.get('GCS_HELPERS_BAND_ORDERING',FIRST)
REMOTE_HEAD=r'^(gs|http|https)://'
GCS_ROOTS=r'^(storage\.googleapis\.com|storage\.cloud\.google\.com)/'


def bucket_key_from_path(path):
    path=re.sub(REMOTE_HEAD,'',path)
    path=re.sub(GCS_ROOTS,'',path)
    parts=path.split('/')
    bucket=parts[0]
    key="/".join(parts[1:])
    return bucket, key


def blob(
        path=None,
        bucket=None,
        key=None,
        dest=None,
        dest_folder=None,
        ext=None,
        as_data=False,
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
        path=None,
        bucket=None,
        key=None,
        dest=None,
        dest_folder=None,
        ext='tif',
        write_mode='wb',
        project=None,
        return_data=True,
        remove_data=True,
        return_dest_with_data=False,
        return_profile=True,
        client=None,
        **read_image_kwargs):
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
        data=_read_image(dest,return_profile=return_profile,**read_image_kwargs)
        if remove_data:
            os.remove(dest)
        if (not remove_data) and return_dest_with_data:
            return data, dest
        else:
            return data
    else:
        return dest


def csv(
        path=None,
        bucket=None,
        key=None,
        dest=None,
        dest_folder=None,
        ext='csv',
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
def _read_image(
        path,
        window=None,
        window_profile=True,
        return_profile=True,
        res=None,
        scale=None,
        out_shape=None,
        bands=None,
        resampling=Resampling.bilinear,
        band_ordering=None,
        dtype=None):
    """ read image: duplicate of imagebox.io.read
    Args: 
        - path<str>: source path
        - window<tuple|Window>: col_off, row_off, width, height
        - window_profile<bool>:
            - if True return profile for the window data
            - else return profile for the src-image
        - res<int>: rescale to new resolution. overides scale and out_shape
        - scale<float>: rescale image res=>res*scale overrides out_shape
        - out_shape<tuple>: (h,w) rescales image. overwritten by res and scale
        - dtype<str>:
    Returns:
        <tuple> np.array, image-profile
    """
    with rio.open(path,'r') as src:
        if return_profile:
            profile=src.profile
        if window:
            w,h=window[2], window[3]
            window=Window(*window)
            if window_profile and return_profile:
                profile['transform']=src.window_transform(window)
                profile['width']=w
                profile['height']=h
        else:
            w,h=src.width, src.height
        if res:
            scale=src.res[0]/res
        if scale:
            out_shape=(int(h*scale),int(w*scale))
        if out_shape and return_profile:
            profile=_rescale_profile(profile,out_shape)
        image=src.read(
                indexes=bands,
                window=window,
                out_shape=out_shape,
                resampling=resampling )
        if dtype:
            image=image.astype(dtype)
        image=_order_bands(image,band_ordering)
    if return_profile:
        return image, profile
    else:
        return image


def _rescale_profile(profile,out_shape):
    """ rescale profile: duplicate of imagebox.io.rescale_profile """
    affine=profile['transform']
    h_out,w_out=out_shape
    h,w=profile['height'],profile['width']
    res_y=int(affine.e*h/h_out)
    res_x=int(affine.a*w/w_out)
    profile['transform']=Affine(res_x, 0.0, affine.c,0.0, res_y, affine.f)
    profile['height'],profile['width']=h_out,w_out
    return profile


def _order_bands(image,band_ordering=None):
    if band_ordering is None:
        band_ordering=BAND_ORDERING 
    if band_ordering.lower()==LAST:
        image=image.transpose(1,2,0)
    return image







