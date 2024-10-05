"""

Create an instance inventory of all AWS accounts, and store it as a JSON.
Returns : No of instances created, terminated in each account compared to previous run.
Author: Sudharshan
"""

import json
import os
import pickle
from datetime import date
from functools import wraps

import boto3
import dateutil


def get():
    old_instance_id = None
    try:
        with open(os.getcwd() + "/data/instance_inventory.pickle", "rb") as pickle_in:
            old_instance_id = pickle.load(pickle_in)
    except FileNotFoundError:
        print("Init")
    finally:
        return old_instance_id


def catch_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AttributeError:
            print("Attribute Error at {}".format(func.__name__))
            return None

    return wrapper


def store(instance_id):
    with open(os.getcwd() + "/data/instance_inventory.pickle", "wb") as pickle_out:
        pickle.dump(instance_id, pickle_out)


def get_ec2_instances(ec2):
    return ec2.instances.filter()


@catch_exception
def get_instance_type(details):
    return details.instance_type


@catch_exception
def get_instance_lifecycle(details):
    return details.instance_lifecycle


@catch_exception
def get_instance_launch_time(details):
    return details.launch_time.date().isoformat()


@catch_exception
def get_instance_availability_zone(details):
    return details.placement["AvailabilityZone"]


@catch_exception
def get_instance_platform(details):
    return details.platform


@catch_exception
def get_instance_private_ip_address(details):
    return details.private_ip_address


@catch_exception
def get_instance_public_dns_name(details):
    return details.public_dns_name


@catch_exception
def get_instance_public_ip_address(details):
    return details.public_ip_address


@catch_exception
def get_instance_security_groups(details):
    sg_names = list()
    for sg in details.security_groups:
        sg_names.append(sg["GroupName"])
    return ", ".join(sg_names)


@catch_exception
def get_instance_state(details):
    return details.state["Name"]


@catch_exception
def get_instance_subnet_id(details):
    return details.subnet_id


@catch_exception
def get_instance_vpc_id(details):
    return details.vpc_id

@catch_exception
def get_creation_date(details, launch_time):
    root_volume_attach_date = None
    nif_date = None
    root_volume = details.root_device_name
    for block_device in details.block_device_mappings:
        if block_device["DeviceName"] == root_volume:
            root_volume_attach_date = block_device["Ebs"]["AttachTime"].date()

    for nw_if in details.network_interfaces_attribute:
        d = nw_if["Attachment"]["AttachTime"]
        if nif_date:
            if d < nif_date:
                nif_date = d
        else:
            nif_date = d
    nif_date = nif_date.date()
    launch_time = dateutil.parser.parse(launch_time).date()

    if root_volume_attach_date < nif_date:
        if root_volume_attach_date < launch_time:
            return root_volume_attach_date.isoformat()
    elif nif_date < launch_time:
        return nif_date.isoformat()
    else:
        return launch_time.isoformat()


def get_ec2_instance_details(ec2, instance_ids):
    inventory = dict()
    for instance_id in instance_ids:
        # print(instance_id)
        details = ec2.Instance(instance_id)
        inventory[instance_id] = dict()
        inventory[instance_id]["Instance Type"] = get_instance_type(details)
        inventory[instance_id]["Instance Lifecycle"] = get_instance_lifecycle(details)
        inventory[instance_id]["Launch Time"] = get_instance_launch_time(details)
        inventory[instance_id]["Availability Zone"] = get_instance_availability_zone(details)
        inventory[instance_id]["Platform"] = get_instance_platform(details)
        inventory[instance_id]["Private IP"] = get_instance_private_ip_address(details)
        inventory[instance_id]["Public DNS Name"] = get_instance_public_dns_name(details)
        inventory[instance_id]["Public IP"] = get_instance_public_ip_address(details)
        inventory[instance_id]["Security Group Name"] = get_instance_security_groups(details)
        inventory[instance_id]["State"] = get_instance_state(details)
        inventory[instance_id]["Subnet ID"] = get_instance_subnet_id(details)
        inventory[instance_id]["VPC ID"] = details.vpc_id
        inventory[instance_id]["Creation Date"] = get_creation_date(details, inventory[instance_id]["Launch Time"])

        try:
            inventory[instance_id]["tag"] = dict()
            for tag in details.tags:
                inventory[instance_id]["tag"][tag["Key"]] = tag["Value"]
        except AttributeError:
            # print("Attribute Error")
            pass

    return inventory


def get_eip(ec2_client):
    eips = ec2_client.describe_addresses()
    eip_instances = set()
    for each_address in eips["Addresses"]:
        try:
            eip_instances.add(each_address["InstanceId"])
        except KeyError:
            # print("Key Error at get_eip")
            pass

    return eip_instances


def main():
    os.environ["AWS_CONFIG_FILE"] = os.path.join(os.getcwd(), ".aws/config")
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(os.getcwd(), ".aws/credentials")

    profiles = ["inventory_ca1", "inventory_ca2", "inventory_ca3", "inventory_ca4", "inventory_ca5"]
    regions = ["ap-south-1", "ap-southeast-1"]
    # profiles =  ["inventory_ca4"]
    instance_inventory = dict()
    old_instance_inventory = get()
    # old_instance_inventory = None
    today = date.today().isoformat()

    for profile in profiles:
        account = profile[profile.index("_")+1:]
        print("Collecting details for {}".format(account))
        instance_inventory[account] = dict()
        session = boto3.session.Session(profile_name=profile)
        current_instance_id = set()

        for region in regions:
            print("For region -> {}".format(region))
            ec2_client = session.client('ec2', region_name=region)
            ec2 = session.resource('ec2', region_name=region)
            instances = get_ec2_instances(ec2)
            eip_instance_id = get_eip(ec2_client)

            curr = set()
            for an_instance in instances:
                current_instance_id.add(an_instance.id)
                curr.add(an_instance.id)

            instance_inventory[account].update(get_ec2_instance_details(ec2, curr))


        for instance_id in current_instance_id:
            if instance_id in eip_instance_id:
                instance_inventory[account][instance_id]["EIP"] = "YES"
            else:
                instance_inventory[account][instance_id]["EIP"] = "NO"

        if old_instance_inventory:
            old_instance_id = set(old_instance_inventory[account].keys())
        else:
            old_instance_id = None

        if old_instance_id:
            deleted_instances = old_instance_id - current_instance_id
            new_instances = current_instance_id - old_instance_id
            print("Deleted instances in {} {}".format(account, deleted_instances))
            if deleted_instances:
                no_deleted_instances = len(deleted_instances)
                print("Number of del instances in {} is {}".format(account, no_deleted_instances))
            print("New instances {}".format(new_instances))
            if new_instances:
                no_new_instances = len(new_instances)
                print("Number of new instances in {} is {}".format(account, no_new_instances))

    # print(instance_inventory)

    with open(os.path.join(os.getcwd(), "data", "instance-" + today + ".json"), "w") as f:
        f.write(json.dumps(instance_inventory))
    store(instance_inventory)


if __name__ == "__main__":
    main()


"""
Todo - name level filter
"""