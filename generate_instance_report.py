"""
Generate team wise instance report
Author: Author Name
"""

import json
import os
import smtplib
from datetime import date
from datetime import datetime
from datetime import timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd


def make_smtp_message(report_path, team_name, cc, recipients):
    today = datetime.today()
    month = datetime.strftime(today - timedelta(15), "%B")
    year = today.year
    smtp_message = MIMEMultipart()
    smtp_message['From'] = "sudharshan.ks@perfios.com"
    smtp_message['To'] = ",".join(recipients)
    smtp_message['cc'] = ",".join(cc)
    smtp_message['Subject'] = "Server Cost Report - {} {} - {}".format(month, year, team_name)
    email_body = """Hey,

PFA, the server cost report for the month of """ + month + """.
If there are any servers that don't belong in the listing. please let us know. We'll exclude them from the next run.
If any servers are missing in the listing, please let us know. We'll include them from the next run.

If there are servers running below 50% usage during an entire month and if you do not foresee a huge spike, then it is advisable to downsize the servers.
Please allow a lead time of 48 hours for downsizing or upgrading the server.


Team Devops"""
    smtp_message.attach(MIMEText(email_body, 'plain'))

    part = MIMEBase("application", "octet-stream")
    attachment = b""
    print("Attaching payload")
    with open(report_path, "rb") as f:
        attachment += f.read()

    part.set_payload(attachment)

    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        "attachment; filename= " + team_name + "_cost_report.csv",
    )
    smtp_message.attach(part)

    return smtp_message


def send_msg(host, port, username, password, message):
    with smtplib.SMTP(host=host, port=port) as server:
        # server.set_debuglevel(2)
        server.starttls()
        server.login(username, password)
        server.sendmail(message['From'], message['To'], message.as_string())


def generate_team_inventory_report(d, teams):
    regions = {
        "ap-south-1": "Mumbai",
        "ap-southeast-1": "Singapore",
    }
    server_criticality = dict()
    for account in d:
        for instance in d[account]:
            try:
                name = d[account][instance]["tag"]["Name"]
            except KeyError:
                name = None
            try:
                team = d[account][instance]["tag"]["Team"].lower()
                for team_name in teams:
                    if team_name.lower() in team:
                        try:
                            teams[team_name]["servers"].add(name)
                        except KeyError:
                            teams[team_name]["servers"] = set()
                            teams[team_name]["servers"].add(name)
            except KeyError:
                team = None
            try:
                description = d[account][instance]["tag"]["Description"].lower()
                for team_name in teams:
                    if team_name.lower() in description:
                        try:
                            teams[team_name]["servers"].add(name)
                        except KeyError:
                            teams[team_name]["servers"] = set()
                            teams[team_name]["servers"].add(name)
            except KeyError:
                description = None
            try:
                product = d[account][instance]["tag"]["Product"].lower()
                for team_name in teams:
                    if team_name.lower() in product:
                        try:
                            teams[team_name]["servers"].add(name)
                        except KeyError:
                            teams[team_name]["servers"] = set()
                            teams[team_name]["servers"].add(name)
            except KeyError:
                product = None
            try:
                criticality = d[account][instance]["tag"]["Server Criticality"].lower()
                try:
                    server_criticality[name]["criticality"] = criticality
                except KeyError:
                    server_criticality[name] = dict()
                    server_criticality[name]["criticality"] = criticality

            except KeyError:
                criticality = ""
                server_criticality[name] = dict()
                server_criticality[name]["criticality"] = criticality

            try:
                az = d[account][instance]["Availability Zone"].lower()
                try:
                    if not az[-1].isdigit():
                        az = regions[az[:-1]]
                except KeyError:
                    print("Region {} not found in dict".format(az))
                try:
                    server_criticality[name]["az"] = az
                except KeyError:
                    server_criticality[name] = dict()
                    server_criticality[name]["az"] = az

            except KeyError:
                az = None

    return teams, server_criticality


def main():
    os.environ["AWS_CONFIG_FILE"] = os.path.join(os.getcwd(), ".aws/config")
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(os.getcwd(), ".aws/credentials")

    today = date.today().isoformat()
    smtp_relay_host = "smtp.pepipost.com"
    smtp_relay_port = 587
    smtp_relay_username = "gitlabmailer"
    smtp_relay_password = "DpU4W4oek#9*7x"
    servers_covered = 0
    total_servers = 0
    with open(os.path.join(os.getcwd(), "data", "instance-" + today + ".json"), "r") as f:
        instance_report = json.load(f)

    with open(os.path.join(os.getcwd(), "team_config.json"), "r") as f:
        config = json.load(f)

    teams = config["teams"]
    general_exclude = config["exclude"]
    report_destination_dir = os.path.join(os.getcwd(), "reports")
    cc_list = config["cc_list"]
    teams, server_criticality = generate_team_inventory_report(instance_report, teams)
    import pprint
    pprint.pprint(teams)
    cost_df = pd.read_csv(os.path.join(os.getcwd(), "data", date.today().isoformat() + ".csv"))

    server_criticality_list = []
    all_servers = set()
    for server in server_criticality:
        server_criticality_list.append((server, (server_criticality[server]["criticality"]).capitalize(), server_criticality[server]["az"]))
        all_servers.add(server)
    total_servers += len(server_criticality_list)
    criticality_df = pd.DataFrame(server_criticality_list, columns=["Server", "Criticality", "Region"])

    server_names_covered = set()
    this_month_servers = set()
    for team in teams:
        l = []
        team_exclude = config["teams"][team]["exclude"]
        recipients = config["teams"][team]["recipients"]
        team_name = (config["teams"][team]["team_name"])
        # print(team_name)
        for server in teams[team]["servers"]:
            if server not in general_exclude and server not in team_exclude:
                try:
                    s = cost_df.loc[cost_df["ResourceName"] == server]["Cost"]
                    l.append((server, round(s.values[0], 2)))
                    server_names_covered.add(server)
                except IndexError:
                    #todo
                    this_month_servers.add(server)
                    # print("{} is not found in cost_df".format(server))
                    pass
        df = pd.DataFrame(l, columns=["Server", "Monthly Cost (USD)"])
        servers_covered += len(l)
        df = df.merge(criticality_df, on="Server")
        report_path = os.path.join(report_destination_dir, team_name.lower() + "_servers_monthly_cost.csv")
        df.to_csv(report_path, index=False)

        print("Making message for " + team_name)
        smtp_message = make_smtp_message(report_path, team_name, cc_list, recipients)
        print("Sending message for " + team_name)
        send_msg(smtp_relay_host, smtp_relay_port, smtp_relay_username, smtp_relay_password, smtp_message)
    print("Total Numbers Serves Covered [{}]/[{}]".format(servers_covered, total_servers))
    pprint.pprint((all_servers - server_names_covered) - this_month_servers )


if __name__ == "__main__":
    main()
