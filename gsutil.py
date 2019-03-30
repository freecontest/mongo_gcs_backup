#!/usr/bin/env python3

import os
import tempfile
import subprocess

import utils

def bucket_upload(src: str, bucket: str, dst: str) -> bool:
    err = utils.validate_object_name(dst)
    if err is not None:
        print(f"Invalid destination filename: {err}")
        return False

    process = subprocess.run([
        "gsutil",
        "-q",
        "cp",
        src,
        f"gs://{bucket}/{dst}"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        print("CalledProcessError encountered while uploading dump to GCS")
        print(f"Return code: {e.returncode}")
        print(f"stdout: '{e.stdout}'")
        print(f"stderr: '{e.stderr}'")
        
        return False
    else:
        return True

def bucket_write_content(bucket: str, dst: str, content: bytes) -> bool:
    err = utils.validate_object_name(dst)
    if err is not None:
        print(f"Invalid destination filename: {err}")
        return False

    _, temp_filename = tempfile.mkstemp()
    with open(temp_filename, "wb") as f:
        f.write(content)
    
    result = bucket_upload(f.name, bucket, dst)

    os.remove(temp_filename)
    return result
    

def bucket_download(bucket, src, dst):
    process = subprocess.run([
        "gsutil",
        "-q",
        "cp",
        f"gs://{bucket}/{src}",
        dst,
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        print("CalledProcessError encountered while uploading dump to GCS")
        print(f"Return code: {e.returncode}")
        print(f"stdout: '{e.stdout}'")
        print(f"stderr: '{e.stderr}'")
        
        return False
    else:
        return True

def bucket_read_content(bucket: str, src: str) -> bytes:
    _, temp_filename = tempfile.mkstemp()

    if bucket_download(bucket, src, temp_filename):
        # TODO: raise something?
        return None

    with open(temp_filename, "rb") as f:
        content = f.read()

    os.remove(temp_filename)
    return content
