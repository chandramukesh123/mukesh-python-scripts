"""
Get high volume EBS

Execution:
    python3 get_high_ebs.py [-t/--threshold] value(int)
Author: Sudharshan
"""

import boto3
import argparse


def get_high_volume_ebs(volumes, threshold):
    for volume in volumes:
        if volume["Size"] > threshold:
            print(volume["VolumeId"], end=" -> ")
            try:
                for tag in volume["Tags"]:
                    if tag["Key"] == "Name":
                        print(tag["Value"])
            except KeyError:
                print("No Name tag found")


def get_unused_ebs(volumes):
    for volume in volumes:
        if len(volume['Attachments']) == 0 and volume['State'] == 'available':
            print(volume["VolumeId"], end=" -> ")
            try:
                for tag in volume["Tags"]:
                    if tag["Key"] == "Name":
                        print(tag["Value"])
            except KeyError:
                print("No name tag found")


def main():
    parser = argparse.ArgumentParser(description="EBS Information")
    parser.add_argument("-u", "--unused", help="Lists unused volumes", action="store_true")
    parser.add_argument("-t", "--threshold", help="Lists volumes beyond threshold GB", metavar="", action="store_const", const=0)
    args = parser.parse_args()

    profiles = ["inventory_ca1", "inventory_ca2", "inventory_ca3", "inventory_ca4"]
    for profile in profiles:
        account = profile[profile.rindex("_") + 1:]
        session = boto3.session.Session(profile_name=profile)
        ec2_client = session.client('ec2', region_name="ap-south-1")
        volumes = ec2_client.describe_volumes()["Volumes"]
        if args.threshold is not None:
            print("\n\nVolumes greater than [{}]GB in [{}] ----------------------".format(args.threshold, account))
            get_high_volume_ebs(volumes, args.threshold)
        if args.unused:
            print("\n\nUnused volumes in [{}] ----------------------".format(account))
            get_unused_ebs(volumes)


if __name__ == "__main__":
    main()
