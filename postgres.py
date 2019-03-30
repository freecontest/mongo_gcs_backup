#!/usr/bin/env python3

import subprocess

def create_dump(db_name: str,
                dump_destination: str,
                db_host: str = None,
                db_username: str = None,
                db_password: str = None):

    args_valid = (
        ((db_host is None) and (db_username is None) and (db_password is None)) or
        ((db_host is not None) and (db_username is not None) and (db_password is not None))
    )
    
    if not args_valid:
        print("invalid args, db_host, db_username, db_password must be None or fully specified")
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
            "--dbname", f"postgres://{db_username}:{db_password}@{db_host}/{db_name}"
        ])
    
    process = subprocess.run(
        pg_dump_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        print("CalledProcessError encountered while dumping database")
        print(f"Return code: {e.returncode}")
        print(f"stdout: '{e.stdout}'")
        print(f"stderr: '{e.stderr}'")
        
        return False
    
    return True

def restore_dump(db_name: str,
                 dump_file: str,
                 db_host: str = None,
                 db_username: str = None,
                 db_password: str = None):
    
    args_valid = (
        ((db_host is None) and (db_username is None) and (db_password is None)) or
        ((db_host is not None) and (db_username is not None) and (db_password is not None))
    )
    
    if not args_valid:
        print("invalid args, db_host, db_username, db_password must be None or fully specified")
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
            "--dbname", f"postgres://{db_username}:{db_password}@{db_host}/{db_name}"
        ])

    process = subprocess.run(
        pg_restore_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8"
    )

    try:
        process.check_returncode()
    except subprocess.CalledProcessError as e:
        print("CalledProcessError encountered while restoring database")
        print(f"Return code: {e.returncode}")
        print(f"stdout: '{e.stdout}'")
        print(f"stderr: '{e.stderr}'")
        
        return False

    return True
