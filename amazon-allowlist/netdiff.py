#! /usr/bin/env python3
# Calculates the disjunction of two sets of IP ranges
from sys import argv
from netaddr import IPSet

if len(argv) != 3:
    print('Usage: {0} include.txt exclude.txt'.format(argv[0]))
    exit()

net = IPSet()

with open(argv[1], 'r') as incfile:
    for line in incfile:
        net = net | IPSet([line])

with open(argv[2], 'r') as exfile:
    for line in exfile:
        net.remove(line)

for cidr in net.iter_cidrs():
    print(cidr)
