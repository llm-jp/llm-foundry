#!/bin/bash

set -e

cat $1 | sed -r "s/^.+ //" | sed -r "s|[^/]+$||" | sort | uniq | xargs -n 1 mkdir -p
cat $1 | xargs -n 2 ln -s
