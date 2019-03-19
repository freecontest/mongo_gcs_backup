#!/usr/bin/env python

import os
import logging
import pathlib
import datetime
import subprocess
import tempfile

def load_env_variables():
    ENVS = [
        "DB_USERNAME",
        "DB_PASSWORD",
        "DB_NAME",
        "GCS_BUCKET"
    ]

    params = {}

    for env_name in ENVS:
        try:
            params[env_name] = os.environ[env_name]
        except KeyError:
            print(f"Missing environment variable, bailing: {env_name}")
            exit(1)

    return params

def create_database_dump(db_name, username, password, dump_filename):
    process = subprocess.run([
        "pg_dump",
        "--file", dump_filename,
        "--compress", "9",
        "--format", "plain",
        "--no-owner",
        "--dbname", f"postgres://{username}:{password}@localhost/{db_name}"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        print("CalledProcessError encountered while dumping database")
        print(f"Return code: {e.returncode}")
        print(f"stdout: '{e.stdout}'")
        print(f"stderr: '{e.stderr}'")
        exit(1)

def generate_dump_filename(db_name):
    current_time = datetime.datetime.utcnow()
    generated_filename = current_time.strftime(f"{db_name}-%Y%m%dT%H%M%S.sql.gz")

    return generated_filename

def upload_to_gcs_bucket(dump_filename, gcs_bucket):
    process = subprocess.run([
        "gsutil",
        "-q",
        "cp",
        dump_filename,
        f"gs://{gcs_bucket}/"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        print("CalledProcessError encountered while dumping database")
        print(f"Return code: {e.returncode}")
        print(f"stdout: '{e.stdout}'")
        print(f"stderr: '{e.stderr}'")
        exit(1)

def main():
    params = load_env_variables()

    dump_file_name = generate_dump_filename(params["DB_NAME"])

    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_path = pathlib.Path(temp_dir_name)
        assert temp_path.exists()

        dump_file_path = temp_path / dump_file_name
        create_database_dump(
            params["DB_NAME"],
            params["DB_USERNAME"],
            params["DB_PASSWORD"],
            str(dump_file_path)
        )
        upload_to_gcs_bucket(
            str(dump_file_path),
            params["GCS_BUCKET"],
        )

    exit(0)

if __name__ == "__main__":
    main()
