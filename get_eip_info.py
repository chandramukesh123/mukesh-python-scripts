#!/usr/bin/python3
"""
Description
Author: Sudharshan
"""
import boto3
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import pandas as pd
from datetime import datetime
import argparse
from utilities import bulk_upload

BULK_UPLOAD = True


def get_unassociated_eip(eips):
    for each_address in eips["Addresses"]:
        try:
            each_address["NetworkInterfaceId"]
        except KeyError:
            yield each_address["PublicIp"]


def main():
    parser = argparse.ArgumentParser(description="EIP Information")
    parser.add_argument("-u", "--unassociated", help="Lists Unassociated EIPs", action="store_true")
    args = parser.parse_args()
    now = datetime.now().date().isoformat()
    profiles = ["inventory_ca1", "inventory_ca2", "inventory_ca3", "inventory_ca4"]
    l = []
    for profile in profiles:
        account = profile[profile.rindex("_")+1:]
        session = boto3.session.Session(profile_name=profile)
        ec2_client = session.client('ec2', region_name="ap-south-1")
        eips = ec2_client.describe_addresses()

        if args.unassociated:
            print("\n\nUnassociated in {}-------------".format(account))
            for eip in get_unassociated_eip(eips):
                print(eip)
                l.append((account, eip))

    if args.unassociated:
        df = pd.DataFrame(l, columns=["Account", "EIP"])
        df["Timestamp"] = now
        bulk_upload(df, BULK_UPLOAD)


if __name__ == "__main__":
    main()
