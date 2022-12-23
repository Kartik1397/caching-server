#!/usr/bin/env python3

from dataclasses import dataclass
import logging
import random
import socket
import string
import sys
import requests
import time
import datetime

HOST = 'localhost'
PORT = int(sys.argv[1])

# not meant for any real security, just to filter out invalid requests
SECRET = '9TcCN6Xj4yrCAfRM7dde'
TELEMETRY_URL = 'http://10.0.36.161:8000'

def get_random_token(size: int = 8):
    return "".join(random.choice(string.ascii_letters) for _ in range(size))


def query(command):
    logging.debug("Request: %s", command)
    command+="\n"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(command.encode('ascii'))
        data = s.recv(1024)
        logging.debug("Response: %s", data)
        return data.decode('ascii').rstrip()


def test_echo():
    token = get_random_token()
    response = query(f"ECHO {token}")
    return response == token


def test_set_get():
    query("SET foo bar")
    response = query("GET foo")
    return response == 'bar'


def test_multiple_set_get():
    for i in range(10):
        query(f"SET key_{i} {i}")
    for i in range(10):
        if query(f"GET key_{i} {i}") != str(i):
            return False
    return True

def test_set_tag_count():
    for i in range(10):
        query(f"SET key_{i} {i} hedgehog")
    response = query("COUNT hedgehog")
    return response == "10"


TEST_LIST = [
    test_echo,
    test_set_get,
    test_multiple_set_get,
]

@dataclass
class Result:
    percentage: float

def run_tests():
    results = []
    for test in TEST_LIST:
        try:
            r = test()
            query("FLUSH")
        except Exception as e:
            logging.debug(e)
            results.append(False)
        else:
            results.append(r)

    return Result(
        sum(results) / len(TEST_LIST)
    )

def print_report(result: Result):
    print("=" * 80)
    print("Percentage solved:", "{:.2%}".format(result.percentage))


def get_public_ip():
    try:
        with open('/tmp/ip') as f:
            ip = f.read()
            if len(ip) < 7:
                raise Exception("Failed to get public IP")
    except Exception as e:
        ip = requests.get('https://api.ipify.org').text
        with open('/tmp/ip', 'w+') as f:
            f.write(ip)
    return ip

def get_candidate_name():
    try:
        with open('/candidate_name') as f:
            name = f.read()
            return name.strip()
    except Exception as e:
        return "NA"

def main():
    result = run_tests()
    print_report(result)


if __name__ == '__main__':
    main()
