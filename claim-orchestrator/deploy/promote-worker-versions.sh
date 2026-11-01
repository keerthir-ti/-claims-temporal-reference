#!/bin/sh
# Make BUILD_ID the current version of every worker deployment once its workers are polling. Versioned
# workers receive no work until this has run. In production the release pipeline does this, normally
# after ramping (temporal worker deployment set-ramping-version --percentage N).
ADDR=${TEMPORAL_ADDRESS:-temporal:7233}
for d in claims-lifecycle claims-stp claims-claimspro claims-tsa claims-is; do
  i=0
  until temporal worker deployment set-current-version --address "$ADDR" \
        --deployment-name "$d" --build-id "$BUILD_ID" --yes >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 90 ]; then echo "Could not make $d:$BUILD_ID current (are its workers running?)"; exit 1; fi
    sleep 2
  done
  echo "current version $d -> $BUILD_ID"
done
