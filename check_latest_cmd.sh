#!/usr/bin/env bash

DOWNLOAD="/home/gpudb/qe-orchestration/alien/orca/download-distro.sh"

LATEST_URL=$(${DOWNLOAD} --query --release-id 7.1.6)
LATEST_RPM=$(basename ${LATEST_URL})

CURRENT_RPM=$(rpm -qa | grep gpudb)

if [[ "${CURRENT_RPM}" != "${LATEST_RPM}" ]]; then
  echo "True"
else
  echo "False"
fi