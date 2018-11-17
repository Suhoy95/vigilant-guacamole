#!/bin/bash

python sdfs-client.py \
    --local tmp \
    --loglevel DEBUG \
    --logfile tmp/debug.log \
    --nameserver localhost:8443
