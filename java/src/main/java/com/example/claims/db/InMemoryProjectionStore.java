package com.example.claims.db;

import com.example.claims.contracts.ProjectionRecord;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** For tests and for running without Postgres. */
public class InMemoryProjectionStore implements ProjectionStore {
  public final Map<String, ProjectionRecord> rows = new ConcurrentHashMap<>();

  @Override
  public boolean insertIfAbsent(ProjectionRecord record) {
    return rows.putIfAbsent(ProjectionStore.key(record), record) == null;
  }
}
