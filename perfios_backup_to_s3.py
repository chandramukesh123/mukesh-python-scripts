"""
Generic Backup Script. Backup all the files and subdirectories below base_path.
The tree structure below base_path is maintained and honored in s3.
The files compression and encryption can be enabled, using the config file.
The archiving can also be done at a directory level wrt to base_path
Refer backup_config.json.reference for all options available

Usage:
python3 perfios_backup_to_s3.py [-h] [-c]
optional arguments:
  -h, --help           show this help message and exit
  -c , --config_path   Test Configuration Path


Some Statistics for hash algo
---------------------
64kb file chunk size
---------------------
1gb file - 30 iterations
MD5 --> 2.284169332186381
SHA1 --> 1.6451879580815634

11gb file - 30 iterations
MD5 --> 75.79730973243713
SHA1 --> 77.094673260053

---------------------
1mb file chunk size of 1gb file
---------------------
MD5 --> 1.5323671261469523
SHA1 --> 1.0385167678197225

---------------------
10mb file chunk size of 1gb file
---------------------
MD5 --> 1.4076042493184409
SHA1 --> 1.1006302038828533

---------------------
100mb file chunk size of 1gb file
---------------------
MD5 --> 1.6204222281773886
SHA1 --> 1.3455795208613077

Entire Program Time
30gb base_path
32 mins total time

Author: Sudharshan
"""

import gzip
import hashlib
import json
import logging
import os
import pickle
import re
import tarfile
import time

from argparse import ArgumentParser

from datetime import datetime
from datetime import timedelta
from logging import handlers
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
FORMATTER = logging.Formatter('%(levelname)s:%(asctime)s:%(funcName)s:%(message)s')
FILE_HANDLER = logging.handlers.TimedRotatingFileHandler(os.getcwd() + '/logs/bk.log', when='midnight', interval=1)
FILE_HANDLER.suffix = "%Y-%m-%d"
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)


def file_blocks(location, blocksize=1048576):
    """
    Read the location blocksize bytes. A generator.
    :param location:
    :param blocksize: By default 1mb
    :return: a block
    """
    with open(location, "rb") as f:
        block = f.read(blocksize)
        while len(block) > 0:
            yield block
            block = f.read(blocksize)


def checksum(location):
    """
    Calculate the sha1 of the location file contents
    :param location:
    :return: string: SHA1 of file
    """
    hasher = hashlib.sha1()
    for data in file_blocks(location):
        hasher.update(data)
    return hasher.hexdigest()


def compress(location, tmp):
    """
    Compress the given location using gzip. Store the compressed file in the temporary location
    :param location:
    :param tmp: temporary location used to store the compressed files
    :return: Path of the compressed file in tmp
    """
    filename = location[location.rindex("/") + 1:]
    compressed_path = os.path.join(tmp, filename + ".gz")

    with open(location, "rb") as f_in, gzip.open(compressed_path, "wb") as f_out:
        f_out.writelines(f_in)

    return compressed_path


def encrypt(tmp_location, gpg_id, location=None):
    """
    Encrypt either the compressed file in temporary location or the original file in location using GPG, based on config
    :param tmp_location: Temporary location of the compressed file. This will be used when compress option is true
    :param gpg_id: GPG ID to be used for encryption
    :param location: if compress option is false, the file is taken from the original location, storing the
                     encrypted file in tmp_location
    :return: Path of the compressed file in tmp
    """
    if location:
        filename = location[location.rindex("/") + 1:]
        encrypted_path = os.path.join(tmp_location, filename + ".gpg")
        command = "gpg --always-trust -o '{}' -r {} -e '{}'".format(encrypted_path, gpg_id, location)
    else:
        command = "gpg --always-trust -r '{}' -e '{}'".format(gpg_id, tmp_location)

    if os.WEXITSTATUS(os.system(command)):
        LOGGER.error("Encryption for " + tmp_location + " failed with traceback. Trying again")
        os.remove(tmp_location + ".gpg")
        os.system(command)

    if location:
        return encrypted_path
    else:
        os.remove(tmp_location)
        return tmp_location + ".gpg"


def upload(s3, base_path, bucket_name, s3_prefix_path, location, tmp_location, last_modified):
    """
    Upload the file to S3
    :param s3: s3_client
    :param base_path: Used to create the prefix i.e., to reflect the local file structure
    :param bucket_name: Name of the bucket
    :param s3_prefix_path: Prefix to be used after bucket name
    :param location: Used to create the prefix i.e., to reflect the local file structure
    :param tmp_location: Temporary location from where the file is uploaded. if no compress and no encrypt,
                         location is considered as tmp_location
    :param last_modified: last modified time is added as metadata while upload. Used while restoring the file
    :return: bool
    """
    if not tmp_location:
        tmp_location = location
    file_name = tmp_location[tmp_location.rindex("/") + 1:]
    prefix = location[len(base_path):location.rindex("/")].strip("/")
    if s3_prefix_path:
        if prefix:
            key = s3_prefix_path + "/" + prefix + "/" + file_name
        else:
            key = s3_prefix_path + "/" + file_name
    else:
        if prefix:
            key = prefix + "/" + file_name
        else:
            key = file_name
    try:
        response = s3.upload_file(Filename=tmp_location, Bucket=bucket_name, Key=key,
                                  ExtraArgs={"Metadata": {"Local-Last-Modified": last_modified}})
    except ClientError as e:
        LOGGER.error("Client error for location: " + location + " with tmp_path: " + tmp_location)
        return False
    except FileNotFoundError as e:
        LOGGER.error("Client error for location: " + location + " with tmp_path: " + tmp_location)
    return True


def scan(base_path, include, exclude, test_regex=False, consider_older=0):
    """
    Scan the files in base_path, creating their hashes. Precedence: Exclude has higher precedence than include, i.e.,
    files are first excluded and then included.
    :param base_path:
    :param include: Regex compiled object to search for matching file to include
    :param exclude: Regex compiled object to search for matching file to exclude
    :param test_regex: If True, pretty print the included and excluded files
    :param consider_older: Consider files older than these days
    :return: dict having filename and their respective hashes
    """
    start_scan = time.time()
    file_info = dict()
    delta = datetime.now().date() - timedelta(consider_older)
    file_count, dir_count, exclude_count, include_count = 0, 0, 0, 0
    if test_regex:
        exclude_files, include_files = [], []
    print("{}".format("".join(["-"] * 75)))
    print("Scanning files in {}".format(base_path))
    LOGGER.info("Scanning files in {}".format(base_path))

    def hash_it():
        """
        Calculates the hash of the file.
        :return:
        """
        m_time = datetime.fromtimestamp(os.stat(location).st_mtime).date()
        if m_time < delta:
            file_info[location] = dict()
            file_info[location]["hash"] = checksum(location)
            # print("Time for {} is {}".format(location, str(time.time()-start)))
        else:
            LOGGER.info(
                "Ignored [{}] as it's modified date is within the ignore range. m_time=[{}]".format(location, m_time))

    for root, _, files in os.walk(base_path):
        # print(root, dir, files)
        for file in files:
            location = os.path.join(root, file)
            file_count += 1
            if exclude:
                if exclude.search(file) is None:
                    if include:
                        if include.search(file):
                            hash_it()
                            include_count += 1
                            if test_regex:
                                include_files.append(location)
                        else:
                            exclude_count += 1
                            if test_regex:
                                exclude_files.append(location)
                    else:
                        hash_it()
                        include_count += 1
                        if test_regex:
                            include_files.append(location)
                else:
                    exclude_count += 1
                    if test_regex:
                        exclude_files.append(location)
            else:
                if include:
                    if include.search(file):
                        hash_it()
                        include_count += 1
                        if test_regex:
                            include_files.append(location)
                    else:
                        exclude_count += 1
                        if test_regex:
                            exclude_files.append(location)
                else:
                    hash_it()
                    include_count += 1
                    if test_regex:
                        include_files.append(location)
            print("\rScanned [{}] directories [{}] files, [{}] file included, [{}] files excluded".format(dir_count, file_count, include_count, exclude_count), end="", flush=True)

        dir_count += 1
    end_scan = time.time()
    LOGGER.info("Time taken for hashing = {}, total files = {}".format(end_scan - start_scan, file_count))
    print("\nScan Time [{:.4f}]s".format(end_scan - start_scan, file_count))
    if test_regex:
        print("Files Included")
        import pprint
        pprint.pprint(include_files)
        print("Files Excluded")
        pprint.pprint(exclude_files)
    return file_info


def compare(new, base_path, archive=False, dir_level=None, meta_file_name=None):
    """
    Compare the file hashes generated today with the file hashes generated yesterday.
    :param new: File information dict of today
    :param base_path: string
    :param archive: bool
    :param dir_level: int/None
    :return: list, dictionary. List of changed location, dictionary of changed dictionaries
    """
    if meta_file_name:
        pickle_path = os.path.join(os.getcwd(), "data/" + meta_file_name + ".pkl")
    else:
        pickle_path = os.path.join(os.getcwd(), "data/" + base_path[base_path.rindex("/") + 1:] + ".pkl")
    len_base_path = len(Path(base_path).parents) + 1
    changed_dirs = None
    if archive:
        changed_dirs = dict()
    try:
        with open(pickle_path, "rb") as pickle_in:
            old = pickle.load(pickle_in)
    except FileNotFoundError:
        old = None

    if old:
        changed_files = list()

        for key in new:
            try:
                LOGGER.info("{} --> {} -- {}".format(key, old[key]["hash"], new[key]["hash"]))
                if new[key]["hash"] != old[key]["hash"]:
                    # print("{} --> {} -- {}".format(key, old[key]["hash"], new[key]["hash"]))
                    changed_files.append(key)
                    if archive:
                        location = Path(key)
                        index = len(location.parents) - len_base_path - dir_level
                        index = index if index > 0 else 0
                        archive_dir_path = str(location.parents[index])
                        try:
                            changed_dirs[archive_dir_path].append(key)
                        except KeyError:
                            changed_dirs[archive_dir_path] = list()
                            changed_dirs[archive_dir_path].append(key)

            except KeyError:
                changed_files.append(key)
                if archive:
                    location = Path(key)
                    index = len(location.parents) - len_base_path - dir_level
                    index = index if index > 0 else 0
                    archive_dir_path = str(location.parents[index])
                    try:
                        changed_dirs[archive_dir_path].append(key)
                    except KeyError:
                        changed_dirs[archive_dir_path] = list()
                        changed_dirs[archive_dir_path].append(key)

        return changed_files, changed_dirs

    else:
        if archive:
            for key in new:
                location = Path(key)
                index = len(location.parents) - len_base_path - dir_level
                index = index if index > 0 else 0
                archive_dir_path = str(location.parents[index])
                try:
                    changed_dirs[archive_dir_path].append(key)
                except KeyError:
                    changed_dirs[archive_dir_path] = list()
                    changed_dirs[archive_dir_path].append(key)

        return new.keys(), changed_dirs


def clean_up(t):
    """
    Removes the location l. Similar to rm -f t
    :param t: location
    :return:
    """
    if t and os.path.exists(t):
        os.remove(t)


def clean_up_empty_directories(base_path):
    command = "find {}/ -type d -empty -delete".format(base_path)
    LOGGER.info("Deleting Empty Directories below {}".format(base_path))
    if os.WEXITSTATUS(os.system(command)):
        os.system(command)


def fetch_optional_config(config, key, default):
    """
    Fetch the value of key in config file if present, else return default value
    :param config: dict
    :param key: string
    :param default: bool/ None/ int as required
    :return: value of key in config
    """
    try:
        return config[key]
    except KeyError:
        return default


def main():
    os.environ["AWS_CONFIG_FILE"] = os.path.join(os.getcwd(), ".aws/config")
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(os.getcwd(), ".aws/credentials")

    # print(os.getenv("AWS_CONFIG_FILE"), os.getenv("AWS_SHARED_CREDENTIALS_FILE"))
    start_entire = time.time()

    parser = ArgumentParser()
    parser.add_argument("-c", "--config", help="Test Configuration Path", type=str, metavar="", dest="config_path", default=None)
    args = parser.parse_args()
    if args.config_path:
        config_file = args.config_path
    else:
        config_file = os.path.join(os.getcwd(), "backup_config.json")

    LOGGER.info("Reading config from {}".format(config_file))
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print("Configuration file Not found.")
        LOGGER.exception("Configuration file Not found. Stack trace")
        exit(0)
    except json.decoder.JSONDecodeError:
        print("Configuration file is not a valid json file.")
        LOGGER.exception("Configuration file is not a valid json file. Stack trace")
        exit(0)

    for _, a_config in config.items():
        # Required Configurations
        base_path = a_config["base_path"].rstrip("/")
        bucket_name = a_config["bucket_name"]
        s3_prefix_path = a_config['s3_prefix_path'].rstrip("/")
        do_compress = a_config["compress"]
        do_encrypt = a_config["encrypt"]
        s3_upload = a_config["s3_upload"]
        aws_profile = a_config["aws_profile"]
        tmp_path = a_config["tmp_path"]

        # Optional Configurations
        gpg_id = fetch_optional_config(a_config, "gpg_id", default=None)
        consider_older = fetch_optional_config(a_config, "consider_older", default=0)
        test_regex = fetch_optional_config(a_config, "test_regex", default=False)
        ignore_case = fetch_optional_config(a_config, "ignore_case", default=False)
        include = fetch_optional_config(a_config, "include", default=None)
        exclude = fetch_optional_config(a_config, "exclude", default=None)
        if ignore_case:
            include = re.compile(include, re.IGNORECASE) if include else None
            exclude = re.compile(exclude, re.IGNORECASE) if exclude else None
        else:
            include = re.compile(include) if include else None
            exclude = re.compile(exclude) if exclude else None
        archive = fetch_optional_config(a_config, "archive", default=False)
        dir_level = None
        if archive:
            dir_level = fetch_optional_config(a_config, "dir_level", default=None)
            test_archive = fetch_optional_config(a_config, "test_archive", default=False)
        delete_source = fetch_optional_config(a_config, "delete_source", default=False)
        delete_empty_dirs = fetch_optional_config(a_config, "delete_empty_dirs", default=False)
        meta_file_name = fetch_optional_config(a_config, "meta_file_name", default=None)
        # Test regex and exit
        if test_regex:
            scan(base_path, include, exclude, test_regex)

        else:
            file_info = scan(base_path, include, exclude, consider_older=consider_older)
            changed_locations, changed_dirs = compare(file_info, base_path, archive, dir_level, meta_file_name)

            LOGGER.info("Changed files are " + str(list(changed_locations)))
            count_changed_locations = len(changed_locations)
            print("Number of Changed files are [{}]".format(str(count_changed_locations)))

            session = boto3.session.Session(profile_name=aws_profile)
            s3 = session.client("s3")
            count_compressed, count_encrypted, count_uploaded = 0, 0, 0

            if archive:
                count_archived = 0

                LOGGER.info("Changed Directories are {}".format(changed_dirs))
                count_changed_dirs = len(changed_dirs)
                print("Number of Changed Directories are [{}]".format(count_changed_dirs))

                # Test archive and exit
                if test_archive:
                    import pprint
                    print("Archiving happens at")
                    pprint.pprint(changed_dirs)
                    exit(0)

                for directory in changed_dirs:
                    # dir_name = os.path.basename(directory)
                    dir_name = directory[directory.rindex("/") + 1:]
                    archive_path = os.path.join(tmp_path, dir_name + ".tar.gz")

                    with tarfile.open(archive_path, "w:gz") as archiver:
                        LOGGER.info("Creating Archive at {}".format(archive_path))
                        for location in changed_dirs[directory]:
                            t = None
                            len_location = len(location)
                            if do_compress:
                                LOGGER.info("Compressing [{}/{}] {}".format(count_compressed, count_changed_locations, location))
                                print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. Compressing {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, location), end="", flush=True)
                                t = compress(location, tmp_path)
                                count_compressed += 1
                                print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, " " * (len_location + 15)), end="", flush=True)
                            if do_encrypt:
                                LOGGER.info("Encrypting [{}/{}] {}".format(count_encrypted, count_changed_locations, location))
                                # print('Encrypting ' + location)
                                print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. Encrypting {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, location), end="", flush=True)
                                if t:
                                    t = encrypt(t, gpg_id)
                                else:
                                    t = encrypt(tmp_path, gpg_id, location)
                                count_encrypted += 1
                                print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, " " * (len_location + 15)), end="", flush=True)
                            if t:
                                arc_name = os.path.join(dir_name, location[len(directory) + 1:location.rindex("/")], os.path.basename(t))
                                LOGGER.info("Adding {} to archive at location {}".format(t, arc_name))
                                archiver.add(t, arcname=arc_name, recursive=False)
                                clean_up(t)
                            else:
                                LOGGER.info("Adding {} to archive at location {}".format(location, arc_name))
                                archiver.add(location, arcname=arc_name, recursive=False)

                        count_archived += 1
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, " " * (len_location + 15)), end="", flush=True)
                        LOGGER.info("Archive [{}/{}] for {} created at {}".format(count_archived, count_changed_dirs, directory, archive_path))

                    if s3_upload:
                        LOGGER.info("Uploading archived {}".format(directory))
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. Uploading {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, directory), end="", flush=True)
                        # print('Uploading ' + location)
                        if not upload(s3, base_path, bucket_name, s3_prefix_path, directory, archive_path, datetime.fromtimestamp(os.stat(directory).st_mtime).date().isoformat()):
                            LOGGER.error("Couldn't upload {} in tmp_path {}".format(directory, archive_path))
                            print("Couldn't upload {} in tmp_path {}".format(directory, archive_path))
                        else:
                            count_uploaded += 1
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Archived [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_archived, count_changed_dirs, count_uploaded, count_changed_dirs, " " * (len(directory) + 15)), end="", flush=True)
                    # Todo Uncomment the below
                    clean_up(archive_path)

            # If no archiving is needed
            else:
                for location in changed_locations:
                    t = None
                    len_location = len(location)
                    if do_compress:
                        LOGGER.info("Compressing " + location)
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Uploaded [{}/{}]. Compressing {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_uploaded, count_changed_locations, location), end="", flush=True)
                        t = compress(location, tmp_path)
                        count_compressed += 1
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_uploaded, count_changed_locations, " " * (len_location + 15)), end="", flush=True)
                    if do_encrypt:
                        LOGGER.info('Encrypting ' + location)
                        # print('Encrypting ' + location)
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Uploaded [{}/{}]. Encrypting {}".format(
                            count_compressed, count_changed_locations, count_encrypted, count_changed_locations,
                            count_uploaded, count_changed_locations, location), end="", flush=True)
                        if t:
                            t = encrypt(t, gpg_id)
                        else:
                            t = encrypt(tmp_path, gpg_id, location)
                        count_encrypted += 1
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_uploaded, count_changed_locations, " " * (len_location + 15)), end="", flush=True)
                    if s3_upload:
                        LOGGER.info('Uploading ' + location)
                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Uploaded [{}/{}]. Uploading {}".format(
                            count_compressed, count_changed_locations, count_encrypted, count_changed_locations,
                            count_uploaded, count_changed_locations, location), end="", flush=True)
                        # print('Uploading ' + location)
                        if not upload(s3, base_path, bucket_name, s3_prefix_path, location, t,
                                      datetime.fromtimestamp(os.stat(location).st_mtime).date().isoformat()):
                            LOGGER.error("Couldn't upload " + location + " in tmp_path " + t)
                            print("Couldn't upload" + location + " in tmp_path " + t)
                        else:
                            count_uploaded += 1

                        print("\rCompressed [{}/{}], Encrypted [{}/{}], Uploaded [{}/{}]. {}".format(count_compressed, count_changed_locations, count_encrypted, count_changed_locations, count_uploaded, count_changed_locations, " " * (len_location + 15)), end="", flush=True)

                    clean_up(t)
                    # Save todays file info

            if meta_file_name:
                pickle_path = os.path.join(os.getcwd(), "data/" + meta_file_name + ".pkl")
            else:
                pickle_path = os.path.join(os.getcwd(), "data/" + base_path[base_path.rindex("/") + 1:] + ".pkl")
            print("\nCaching Metadata for {}".format(base_path))
            with open(pickle_path, "wb") as pickle_out:
                pickle.dump(file_info, pickle_out)

            if delete_source:
                LOGGER.info("Deleting Source Files Start")
                i = 1
                for location in changed_locations:
                    print("\rDeleted [{}/{}]. Deleting {}".format(i, count_changed_locations, location), end="", flush=True)
                    LOGGER.info("Deleted [{}/{}]. Deleting {}".format(i, count_changed_locations, location))
                    clean_up(location)
                    i += 1
                LOGGER.info("Deleting Source Files Successful")

            if delete_empty_dirs:
                LOGGER.info("Deleting Empty Directories")
                print("\nDeleting Empty Directories")
                clean_up_empty_directories(base_path)
                LOGGER.info("Deleted")

    # Todo Handle Exception in main program, do clean up in except
    print("\n{}".format("".join(["-"] * 75)))
    print("Total Time [{:.4f}]s".format(time.time() - start_entire))
    LOGGER.info("Total Time [{:.4f}]s".format(time.time() - start_entire))


if __name__ == "__main__":
    main()
