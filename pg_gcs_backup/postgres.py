#!/usr/bin/env python3

import logging
import subprocess

from pg_gcs_backup import utils

logger = logging.getLogger(__name__)


def create_dump(db_name: str,
                dump_destination: str,
                db_host: str = None,
                db_username: str = None,
                db_password: str = None) -> bool:
    if not utils.valid_db_args(db_host, db_username, db_password):
        logger.fatal("Invalid database arguments, either all of them must be "
                     "specified or not")
        return False

    pg_dump_args = [
        "pg_dump",
        "--file", dump_destination,
        "--compress", "9",
        "--format", "custom"
    ]

    if (db_host is None) and (db_username is None) and (db_password is None):
        pg_dump_args.append(db_name)
    else:
        pg_dump_args.extend([
            "--dbname",
            f"postgres://{db_username}:{db_password}@{db_host}/{db_name}"
        ])

    process = subprocess.run(
        pg_dump_args,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        encoding = "utf-8"
    )

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        utils.subprocess_error_dump(logger, e)
        return False

    return True


def restore_dump(db_name: str,
                 dump_file: str,
                 db_host: str = None,
                 db_username: str = None,
                 db_password: str = None) -> bool:
    if not utils.valid_db_args(db_host, db_username, db_password):
        logger.fatal("Invalid database arguments, either all of them must be "
                     "specified or not")
        return False

    pg_restore_args = [
        "pg_restore",
        "--clean",
        "--single-transaction",
        dump_file
    ]

    if (db_host is None) and (db_username is None) and (db_password is None):
        pg_restore_args.extend([
            "--dbname", db_name
        ])
    else:
        pg_restore_args.extend([
            "--dbname",
            f"postgres://{db_username}:{db_password}@{db_host}/{db_name}"
        ])

    process = subprocess.run(
        pg_restore_args,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        encoding = "utf-8"
    )

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        utils.subprocess_error_dump(logger, e)
        return False

    return True
