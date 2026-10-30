#!/bin/sh
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Create corpus, warehouse, and run buckets before their first use so missing
# buckets do not surface as misleading writer permission errors.
set -eu
mc alias set local http://minio:9000 admin password
# Allow repeated stack startup.
for b in corpus warehouse runs; do mc mb --ignore-existing "local/$b"; done
