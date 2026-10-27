#!/bin/sh
# Creates Kibana data views and a few saved searches once Kibana is up.
K=http://kibana:5601
# "degraded" still serves the APIs used below (common on small machines while plugins warm up).
until curl -fs "$K/api/status" | grep -qE '"overall":\{"level":"(available|degraded)"'; do echo "waiting for kibana"; sleep 5; done

# POST with retries: a busy Kibana times out individual calls even after reporting ready.
post() {
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    curl -fs -m 30 -X POST "$K$1" -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d "$2" >/dev/null && return 0
    sleep 5
  done
  echo "FAILED $1" >&2
  return 1
}
view() {
  post /api/data_views/data_view \
    "{\"override\":true,\"data_view\":{\"id\":\"$1\",\"title\":\"$2\",\"name\":\"$3\",\"timeFieldName\":\"@timestamp\"}}" \
    && echo "data view $3"
}
view claims-logs "claims-logs-*" "Claims logs (Java + Python)"
view claims-metrics "claims-metrics-*" "Claims Temporal SDK metrics"
search() {
  post "/api/saved_objects/search/$1?overwrite=true" "{
    \"attributes\":{\"title\":\"$2\",\"columns\":[\"service\",\"app.level\",\"app.WorkflowId\",\"app.ActivityType\",\"message\"],
    \"kibanaSavedObjectMeta\":{\"searchSourceJSON\":\"{\\\"query\\\":{\\\"language\\\":\\\"kuery\\\",\\\"query\\\":\\\"$3\\\"},\\\"indexRefName\\\":\\\"kibanaSavedObjectMeta.searchSourceJSON.index\\\"}\"}},
    \"references\":[{\"name\":\"kibanaSavedObjectMeta.searchSourceJSON.index\",\"type\":\"index-pattern\",\"id\":\"claims-logs\"}]}" \
    && echo "saved search $2"
}
search claim-trail "Claim trail (set claim.id)" "claim.id : *"
search transitions "Lifecycle transitions" "app.message : Transition*"
search retries "Activity retries (Attempt > 1)" "app.Attempt > 1"
search dead-letters "Bridge dead letters" "app.message : DEAD_LETTER*"
search stale "Stale-stage rejections" "app.message : *stale*"
echo done
