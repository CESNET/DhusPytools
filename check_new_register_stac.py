#!/usr/bin/env python3
import fcntl
import os
import sys
import glob
import subprocess
import gen_new_list

SENTINEL_DIR = "/var/tmp/sentinel"
SCRIPT_NAME = "check_new_register_stac"
os.makedirs(SENTINEL_DIR, exist_ok=True)

lock_file = os.path.join(SENTINEL_DIR, f"{SCRIPT_NAME}.lock")
list_file = os.path.join(SENTINEL_DIR, "gen_new_list_processed.txt")
err_file_pattern = os.path.join(SENTINEL_DIR, "register-stac-error-*")

class FileLock:
    """
    Object for ensuring a single instance of this script is running.
    Creates locked file, lock is removed when script exits.
    If another process locked the file, this process will terminate immediately when trying to create lock.
    """
    def __init__(self, lockfile):
        self.lockfile = lockfile
        self.dir_fd = os.open(self.lockfile, os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(self.dir_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            sys.stderr.write(
                f"Exiting: Lock file exists: {lock_file}\n\"{SCRIPT_NAME}\" is only meant to be run once at a time.\n\n")
            sys.exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # the file itself should probably not be removed (https://unix.stackexchange.com/a/368167)
        fcntl.flock(self.dir_fd, fcntl.LOCK_UN)
        os.close(self.dir_fd)

def main():
    FileLock(lock_file)  # create lock, will fail if script is already running
    error_files = glob.glob(err_file_pattern)
    newest_err_file = max(error_files, key=os.path.getmtime) if error_files else None

    print("Fetching products")
    try:
        gen_new_list.main()
    except Exception:
        sys.stderr.write(f"Failed to fetch products.")
        sys.exit(1)

    if os.path.isfile(list_file):
        with open(list_file, "r") as lf:
            lines = lf.readlines()
            print(f"Fetched {len(lines)} products")

            for product_id in lines:
                if not product_id:
                    continue
                subprocess.run(["python3", "./register_stac.py", "-p", "-i", product_id.strip()])
    else:
        sys.stderr.write(f"List of fetched products not found: {list_file}")
        sys.exit(1)

    if not newest_err_file:
        print("No previous error file found.")
    else:
        with open(newest_err_file, "r") as ef:
            err_lines = ef.readlines()
            print(f"Previously created error file contains {len(err_lines)} products")
            for line in ef:
                try:
                    parts = line.strip().split(",", 2)
                    if len(parts) < 3:
                        continue
                    _, product_id, error_info = parts
                    error_code_str = error_info.split(":", 1)[0]
                    error_code = int(error_code_str)
                    if error_code >= 400 and error_code != 409:
                        subprocess.run(["python3", "./register_stac.py", "-p", "-i", product_id])
                except Exception as e:
                    print(f"Exception thrown during error file parsing\n{e}")


if __name__ == "__main__":
    main()
