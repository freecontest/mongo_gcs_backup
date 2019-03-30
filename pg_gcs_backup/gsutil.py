#!/usr/bin/env python3

import logging
import os
import subprocess
import tempfile

from pg_gcs_backup import utils

logger = logging.getLogger(__name__)


def bucket_upload(src: str, bucket: str, dst: str) -> bool:
    err = utils.validate_object_name(dst)
    if err is not None:
        logger.fatal(f"Invalid destination filename: {err}")
        return False

    process = subprocess.run([
        "gsutil",
        "-q",
        "cp",
        src,
        f"gs://{bucket}/{dst}"
    ], stdout = subprocess.PIPE, stderr = subprocess.PIPE, encoding = "utf-8")

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        utils.subprocess_error_dump(logger, e)
        return False

    return True


def bucket_write_content(bucket: str, dst: str, content: bytes) -> bool:
    err = utils.validate_object_name(dst)
    if err is not None:
        logger.fatal(f"Invalid destination filename: {err}")
        return False

    _, temp_filename = tempfile.mkstemp()
    with open(temp_filename, "wb") as f:
        f.write(content)

    result = bucket_upload(temp_filename, bucket, dst)

    os.remove(temp_filename)

    return result


def bucket_download(bucket, src, dst):
    process = subprocess.run([
        "gsutil",
        "-q",
        "cp",
        f"gs://{bucket}/{src}",
        dst,
    ], stdout = subprocess.PIPE, stderr = subprocess.PIPE, encoding = "utf-8")

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


def bucket_read_content(bucket: str, src: str) -> bytes or None:
    _, temp_filename = tempfile.mkstemp()

    if bucket_download(bucket, src, temp_filename):
        return None

    with open(temp_filename, "rb") as f:
        content = f.read()

    os.remove(temp_filename)
    return content
