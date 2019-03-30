#!/usr/bin/env python3

import datetime
import logging
import subprocess


def get_machine_uuid():
    pass


def generate_dump_filename(db_name):
    current_time = datetime.datetime.utcnow()
    generated_filename = current_time.strftime(
        f"{db_name}-%Y%m%dT%H%M%SZ.pgdump")

    return generated_filename


def validate_object_name(name: str):
    """
    Validates object name based on Google's recommendations:
    https://cloud.google.com/storage/docs/naming

    Returns an error string on failure, None otherwise
    """

    DISALLOWED_CHARACTERS = (
            "\r\n" +
            "#" +
            "[]*?" +
            "".join([chr(c) for c in range(0x7f, 0x84 + 1)]) +
            "".join([chr(c) for c in range(0x86, 0x9f + 1)])
    )

    utf8_len = len(name.encode("utf-8"))
    if utf8_len < 1:
        return "UTF-8 encoded string too short ({utf8_len})"
    if utf8_len > 1024:
        return "UTF-8 encoded string too long ({utf8_len})"

    if name.startswith(".well-known/acme-challenge"):
        return "Disallowed prefix: .well-known/acme-challenge"

    if (name == ".") or (name == ".."):
        return f"Disallowed name: {name}"

    for c in name:
        if c in DISALLOWED_CHARACTERS:
            return f"Disallowed character in name: 0x{c:02x}"

    return None


def valid_db_args(db_host: str = None,
                  db_username: str = None,
                  db_password: str = None) -> bool:
    return (
            (
                    (db_host is None) and
                    (db_username is None) and
                    (db_password is None)
            ) or
            (
                    (db_host is not None) and
                    (db_username is not None) and
                    (db_password is not None)
            )
    )


def subprocess_error_dump(logger: logging.Logger,
                          e: subprocess.CalledProcessError) -> None:
    logger.fatal("CalledProcessError encountered: "
                 "errcode = %s "
                 "cmd = '%s' "
                 "stdout = '%s' "
                 "stderr = '%s'", e.returncode, e.cmd, e.stdout, e.stderr)
