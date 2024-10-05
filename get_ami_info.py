"""
Fetch snapshot/AMI along with date
Author: Sudharshan
"""

import boto3


def get_images(ec2_client):
    images = ec2_client.describe_images(Owners=["self"])["Images"]
    for image in images:
        print("{}\t{}".format(image["Name"], image["CreationDate"]))


def main():
    profiles = ["inventory_ca1", "inventory_ca2", "inventory_ca3", "inventory_ca4"]
    for profile in profiles:
        account = profile[profile.rindex("_") + 1:]
        session = boto3.session.Session(profile_name=profile)
        ec2_client = session.client('ec2', region_name="ap-south-1")
        print(account + "------------------")
        get_images(ec2_client)
        print()


if __name__ == "__main__":
    main()
