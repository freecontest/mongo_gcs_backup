#!/usr/bin/env python3

import argparse
import logging
import sys

from pg_gcs_backup import utils
from pg_gcs_backup.functions import backup, restore


def get_logger(is_verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger()

    if is_verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.CRITICAL)

    formatter = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)

    return logger


def parse_args() -> argparse.Namespace:
    ALLOWED_ACTIONS = ["backup", "restore"]

    parser = argparse.ArgumentParser(
        description = "Automatically backup and restore your PostgreSQL "
                      "database to a GCS bucket"
    )

    parser.add_argument("action", choices = ALLOWED_ACTIONS,
                        help = "Action to take ('backup' or 'restore')")
    parser.add_argument("db_name",
                        help = "Database name to backup/restore from")
    parser.add_argument("bucket",
                        help = "GCD bucket name to backup/restore from")

    parser.add_argument("-H", "--db-host",
                        help = "Database host")
    parser.add_argument("-U", "--db-username",
                        help = "Database username")
    parser.add_argument("-P", "--db-password",
                        help = "Database password")

    parser.add_argument("-D", "--directory",
                        help = "Store backups in this directory inside the "
                               "bucket (default is to store in bucket root)")

    parser.add_argument("-v", "--verbose", action = "store_true",
                        help = "Verbose logging "
                               "(default is to log errors only)")
    parser.add_argument("-y", "--yes", action = "store_true",
                        help = "Assume yes on all prompts")

    args = parser.parse_args()

    if args.action not in ALLOWED_ACTIONS:
        parser.error(f"Invalid action, allowed actions are: "
                     f"{','.join(ALLOWED_ACTIONS)}")

    if not utils.valid_db_args(args.db_host,
                               args.db_username,
                               args.db_password):
        parser.error("Invalid database arguments, either all of them must be "
                     "specified or not")

    return args


def main() -> None:
    args = parse_args()
    logger = get_logger(args.verbose)

    logger.debug("Arguments: %s", str(args))

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


if __name__ == "__main__":
    main()
