"""
Get the Abby Remaining Pages Count
Author: Sudharshan
"""
import json
import os
import re
import socket
import time

import pexpect
import requests


def get_page_count(location):
    child = pexpect.spawn(location)
    child.setwinsize(800, 800)
    child.expect("Quit")
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


def send_message(message):
    web_hook = "https://hooks.slack.com/services/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"
    payload = {"text": message, "username": "abbyy_bot", "icon_emoji": ":zap:"}
    requests.post(web_hook, data=json.dumps(payload))


def main():
    with open(os.path.join(os.getcwd(), "abby.json"), "r") as f:
        d = json.load(f)
    hostname = socket.gethostname()
    for location in d["locations"]:
        pages_remaining, total_pages = get_page_count(location)
        pages_remaining = int(pages_remaining)
        message = "{}\nPages Remaining [{}]/[{}]".format(hostname, pages_remaining, total_pages)
        send_message(message)


if __name__ == '__main__':
    main()
