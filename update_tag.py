"""
Updates the AWS tags based on the config file
Author: Sudharshan
"""
import json
import os
import boto3

with open(os.path.join(os.getcwd(), "data/server_inventory_with_id_acc_wise.json"), "r") as f:
    inventory = json.load(f)

profiles = ["ca1", "ca4", "ca2", "ca3"]
for profile in profiles:
    account = profile
    print("Changing tags in {}".format(account))

    session = boto3.session.Session(profile_name=profile)
    ec2_client = session.client('ec2', region_name="ap-south-1")
    ec2 = session.resource('ec2', region_name="ap-south-1")

    for each_server in inventory[account]:
        print("\tAdding for instance {} tag Team -> {}".format(each_server["name"], each_server["team"]))
        response = ec2_client.create_tags(
            Resources=[each_server["id"]],
            Tags=[{
                "Key": "Team",
                "Value": each_server["team"]
            }]
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print(response["ResponseMetadata"]["HTTPStatusCode"])