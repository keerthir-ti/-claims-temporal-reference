#!/bin/sh
# Promote BUILD_ID to the current version of every worker deployment, once its workers are polling.
# Versioned workers receive no work until this has run. Production: the release pipeline does it,
# usually ramping (--percentage) before making a version current.
ADDR=${TEMPORAL_ADDRESS:-temporal:7233}
for d in claims-lifecycle claims-stp claims-tsa claims-is; do
  i=0
  until temporal worker deployment set-current-version --address "$ADDR" \
        --deployment-name "$d" --build-id "$BUILD_ID" --yes >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then echo "FAILED to promote $d:$BUILD_ID (are its workers running?)"; exit 1; fi
    sleep 2
  done
  echo "current version $d -> $BUILD_ID"
done
