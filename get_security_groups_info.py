"""
Fetches all secruity grps / fetch allow all inbound rules
Author: Sudharshan
"""

import boto3
import pprint


def get_security_groups(ec2_client):
    security_groups = ec2_client.describe_security_groups()["SecurityGroups"]
    rules = set()
    basket = dict()
    ipv6_sg = list()
    for security_group in security_groups:
        in_rules = list()
        out_rules = list()
        ipv6 = True
        for igress in security_group["IpPermissions"]:
            try:
                port = (igress["FromPort"], igress["ToPort"])
            except KeyError:
                port = (-1, -1)
            protocol = igress["IpProtocol"]
            in_ip_range = list()
            for ip in igress["IpRanges"]:
                in_ip_range.append(ip["CidrIp"])
            in_rules.append((port, protocol, tuple(in_ip_range)))
            if ipv6 and len(igress["Ipv6Ranges"]):
                ipv6 = False
                ipv6_sg.append((security_group["GroupId"], security_group["Description"]))

        for ogress in security_group["IpPermissionsEgress"]:
            protocol = ogress["IpProtocol"]
            out_ip_range = list()
            for ip in ogress["IpRanges"]:
                out_ip_range.append(ip["CidrIp"])
            out_rules.append((protocol, tuple(out_ip_range)))
        value = (tuple(in_rules), tuple(out_rules))
        if value in rules:
            basket[value.__hash__()]["count"] += 1
            basket[value.__hash__()]["occurences"].append({"id": security_group["GroupId"], "Name": security_group["Description"]})

        else:
            rules.add(value)
            basket[value.__hash__()] = {'count': 1, "occurences": [{"id": security_group["GroupId"], "Name": security_group["Description"]}]}
    pprint.pprint(basket)
    print("Security Groups [{}], Reducable to [{}]".format(len(security_groups), len(rules)))
    print("SG using IPV6")
    pprint.pprint(ipv6_sg)
    print("{}".format("-"*50))


def main():
    profiles = ["inventory_ca1", "inventory_ca2", "inventory_ca3", "inventory_ca4"]
    for profile in profiles:
        account = profile[profile.rindex("_") + 1:]
        session = boto3.session.Session(profile_name=profile)
        ec2_client = session.client('ec2', region_name="ap-south-1")
        print(account + "--------------")
        get_security_groups(ec2_client)


if __name__ == "__main__":
    main()

"""
demo08.perfios.com-sg
demo25.perfios.com-sg
demo34.perfios.com-sg
demo15.perfios.com-sg
demo30.perfios.com-sg
minivet.hinagro.com-sg
demo38.perfios.com-sg
"""