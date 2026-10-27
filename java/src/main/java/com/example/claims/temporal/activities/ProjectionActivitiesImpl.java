package com.example.claims.temporal.activities;

import com.example.claims.db.ProjectionStore;
import com.example.claims.contracts.ProjectionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectionActivitiesImpl implements ProjectionActivities {
  private static final Logger log = LoggerFactory.getLogger(ProjectionActivitiesImpl.class);
  private final ProjectionStore store;

  public ProjectionActivitiesImpl(ProjectionStore store) {
    this.store = store;
  }

  @Override
  public void recordProjection(ProjectionRecord r) {
    boolean inserted = store.insertIfAbsent(r);
    // Structured line so the same business trail is also searchable in Kibana.
    log.info("projection key={} track={} step={} status={} inserted={} detail={}",
        ProjectionStore.key(r), r.getTrack(), r.getStep(), r.getStatus(), inserted, r.getDetail());
  }
}
