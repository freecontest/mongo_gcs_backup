#!/usr/bin/env python3

import argparse
import os
import logging
import pathlib
import datetime
import subprocess
import tempfile

import postgres
import utils
import gsutil

def parse_args():
    parser = argparse.ArgumentParser(
        description="Automatically backup and restore your PostgreSQL database to a GCS bucket"
    )

    parser.add_argument("action",
                        choices=["backup", "restore"],
                        help="Action to take ('backup' or 'restore')")
    parser.add_argument("db_name", help="Database name to backup/restore from")
    parser.add_argument("bucket", help="GCD bucket name to backup/restore from")

    parser.add_argument("-H", "--db-host",
                        help="Database host")
    parser.add_argument("-U", "--db-username",
                        help="Database username")
    parser.add_argument("-P", "--db-password",
                        help="Database password")
    parser.add_argument("-D", "--directory",
                        help="Store backups in this directory inside the bucket (root folder is the default)")
    parser.add_argument("-y", "--yes",
                        help="Assume yes on all prompts")

    args = parser.parse_args()
    print(args)

    return parser.parse_args()

def backup(db_name: str,
           bucket: str,
           db_host: str = None,
           db_username: str = None,
           db_password: str = None,
           directory: str = None):
    
    _, dump_filename = tempfile.mkstemp(suffix=".pgdump")
    
    postgres.create_dump(
        db_name,
        dump_filename,
        db_host,
        db_username,
        db_password
    )

    dest_filename = utils.generate_dump_filename(db_name)
    if directory is not None:
        dest_fullpath = f"{directory}/{dest_filename}"
    else:
        dest_fullpath = dest_filename

    gsutil.bucket_upload(dump_filename, bucket, dest_fullpath)

    latest_path = "latest.txt"
    if directory is not None:
        latest_path = f"{directory}/latest.txt"

    gsutil.bucket_write_content(bucket, latest_path, dest_filename.encode("utf-8"))

    os.remove(dump_filename)

def restore(db_name: str,
            bucket: str,
            db_host: str = None,
            db_username: str = None,
            db_password: str = None,
            directory: str = None,
            yes: bool = False):

    if not yes:
        print("THIS WILL DROP THE DATABASE NAMED '{db_name}' BEFORE RESTORING")
        print("Are you sure about this (type 'yes' to continue): ")
        if input() != "yes":
            print("Invalid response, aborting")
            return

    latest_path = "latest.txt"
    if directory is not None:
        latest_path = f"{directory}/latest.txt"

    latest_dump = gsutil.bucket_read_content(bucket, latest_path)
    if latest_path is None:
        # TODO: raise something?
        return None
    latest_dump = latest_dump.decode("utf-8")

    _, dump_filename = tempfile.mkstemp(suffix=".pgdump")
    gsutil.bucket_download(bucket, latest_dump, dump_filename)

    postgres.restore_dump(
        db_name,
        dump_filename,
        db_host,
        db_username,
        db_password
    )

def main():
    args = parse_args()

    if args.action == "backup":
        backup(
            args.db_name,
            args.bucket,
            args.db_host,
            args.db_username,
            args.db_password,
            args.directory
        )
    elif args.action == "restore":
        restore(
            args.db_name,
            args.bucket,
            args.db_host,
            args.db_username,
            args.db_password,
            args.directory,
            args.yes
        )
    else:
        print("Invalid action, valids are 'backup' and 'restore'")
        exit(1)

if __name__ == "__main__":
    main()
