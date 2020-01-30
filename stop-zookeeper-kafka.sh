#!/usr/bin/env bash

set -eux

kafka-server-stop.sh || true
zookeeper-server-stop.sh
