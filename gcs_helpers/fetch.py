from io import BytesIO
import secrets
from google.cloud import storage
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
        ext=None,
        as_data=False,
        path=None,
        write_mode='w',
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
            dest=utils.generate_name(dest,ext)
        utils.write_blob(blob,dest,mode=write_mode)
        return dest



