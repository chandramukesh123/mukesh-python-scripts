"""
Get the Abby Remaining Pages Count. Only for abby04.hinagro.com
Author: Sudharshan
"""
import json
import os
import re
import socket
import time

import pexpect
import requests


def get_page_count(location, second_license=False):
    child = pexpect.spawn(location)
    child.setwinsize(800, 800)
    child.expect("Quit")
    if second_license:
        child.send("\033[B")
    child.send("\r")
    time.sleep(1)
    child.expect("Back")
    child.send("\r")
    time.sleep(1)
    child.expect("return \*\*\*")
    output = child.before
    child.send("\r")
    child.terminate()

    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    res = ansi_escape.sub('\n', output.decode())
    res = re.sub(r"\s\s+", " ", res)
    res = res.lower().split(" ")
    pages = res[res.index("remains:") + 1]
    total = res[res.index("quantity:") + 1]

    print("{}/{}".format(pages, total))

    return pages, total


def send_message(message, web_hook):
    payload = {"text": message, "username": "abbyy_bot", "icon_emoji": ":zap:"}
    requests.post(web_hook, data=json.dumps(payload))


def main():
    with open(os.path.join(os.getcwd(), "abby.json"), "r") as f:
        d = json.load(f)
    hostname = socket.gethostname()
    main_channel = "https://hooks.slack.com/services/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"
    alert_abby_channel = "https://hooks.slack.com/services/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"
    for location in d["locations"]:
        pages_remaining, total_pages = get_page_count(location)
        second_result = get_page_count(location, second_license=True)
        pages_remaining = int(pages_remaining)
        total_pages = int(total_pages)
        pages_remaining += int(second_result[0])
        total_pages += int(second_result[1])
        print(pages_remaining, total_pages)
        exit(0)

        pages_remaining_pc = (pages_remaining/total_pages) * 100
        if pages_remaining <= 10000:
            message = ":no_entry: {}\nPages Remaining = [{}]/[{}], {:.1f}%".format(hostname, pages_remaining, total_pages, pages_remaining_pc)
            # send_message(message, alert_abby_channel)

        elif pages_remaining <= 20000:
            message = ":warning: {}\nPages Remaining = [{}]/[{}], {:.1f}%".format(hostname, pages_remaining, total_pages, pages_remaining_pc)
            # send_message(message, alert_abby_channel)

        else:
            message = "{}\nPages Remaining = [{}]/[{}], {:.1f}%".format(hostname, pages_remaining, total_pages, pages_remaining_pc)
        send_message(message, main_channel)
        send_message(message, alert_abby_channel)


if __name__ == '__main__':
    main()
