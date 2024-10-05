"""
Gets the CIDR ranges used over all the accounts
Author: Sudharshan
"""

import boto3


def get_vpc(ec2_client):
    vpcs = ec2_client.describe_vpcs()["Vpcs"]
    for vpc in vpcs:
        try:
            for tag in vpc["Tags"]:
                if tag["Key"] == "Name":
                    print("{}\t{}".format(tag["Value"], vpc["CidrBlock"]))
        except KeyError:
            print("{}\t{}".format(vpc["CidrBlock"], None))


def main():
    profiles = ["inventory_ca1", "inventory_ca2", "inventory_ca3", "inventory_ca4"]
    for profile in profiles:
        account = profile[profile.rindex("_") + 1:]
        session = boto3.session.Session(profile_name=profile)
        ec2_client = session.client('ec2', region_name="ap-south-1")
        print(account + "------------------")
        get_vpc(ec2_client)
        print()


if __name__ == "__main__":
    main()
