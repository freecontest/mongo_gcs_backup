#!/usr/bin/env python3

import logging
import os
import tempfile

from pg_gcs_backup import gsutil, postgres, utils

logger = logging.getLogger(__name__)


def backup(db_name: str,
           bucket: str,
           db_host: str = None,
           db_username: str = None,
           db_password: str = None,
           directory: str = None) -> bool:
    _, dump_filename = tempfile.mkstemp(suffix = ".pgdump")

    logger.info("Generating dump file")

    success = postgres.create_dump(
        db_name,
        dump_filename,
        db_host,
        db_username,
        db_password
    )

    if not success:
        logger.fatal("Unable to dump database")
        return False

    logger.info("Uploading dump file to GCS")

    dest_filename = utils.generate_dump_filename(db_name)
    if directory is not None:
        dest_full_path = f"{directory}/{dest_filename}"
    else:
        dest_full_path = dest_filename

    if not gsutil.bucket_upload(dump_filename, bucket, dest_full_path):
        logger.fatal("Unable to upload dump to GCS bucket")
        os.remove(dump_filename)
        return False

    logger.info("Updating latest.txt")

    latest_full_path = "latest.txt"
    if directory is not None:
        latest_full_path = f"{directory}/latest.txt"

    if not gsutil.bucket_write_content(bucket, latest_full_path,
                                       dest_filename.encode("utf-8")):
        logger.fatal("Unable to update latest.txt")
        os.remove(dump_filename)
        return False

    os.remove(dump_filename)
    return True


def restore(db_name: str,
            bucket: str,
            db_host: str = None,
            db_username: str = None,
            db_password: str = None,
            directory: str = None,
            yes: bool = False) -> bool:
    if not yes:
        print("THIS WILL DROP THE DATABASE NAMED '{db_name}' BEFORE RESTORING")
        print("Are you sure? (type 'yes' to continue): ")
        if input() != "yes":
            print("Invalid response, aborting")
            return False

    logger.info("Fetching latest dump filename from latest.txt")

    latest_full_path = "latest.txt"
    if directory is not None:
        latest_full_path = f"{directory}/latest.txt"

    latest_dump_filename = gsutil.bucket_read_content(bucket, latest_full_path)
    if latest_dump_filename is None:
        logger.fatal("Unable to get latest dump filename")
        return False

    latest_dump_filename = latest_dump_filename.decode("utf-8")
    logger.info("Latest dump is %s", latest_dump_filename)

    logger.info("Fetching latest dump from GCS")

    if directory is not None:
        src_full_path = f"{directory}/{latest_dump_filename}"
    else:
        src_full_path = latest_dump_filename

    _, dump_filename = tempfile.mkstemp(suffix = ".pgdump")
    if not gsutil.bucket_download(bucket, src_full_path, dump_filename):
        logger.fatal("Unable to fetch latest dump from GCS")
        os.remove(dump_filename)
        return False

    logger.info("Restoring dump file")

    success = postgres.restore_dump(
        db_name,
        dump_filename,
        db_host,
        db_username,
        db_password
    )

    if not success:
        logger.fatal("Unable to restore dump")
        os.remove(dump_filename)
        return False

    os.remove(dump_filename)
    return True
