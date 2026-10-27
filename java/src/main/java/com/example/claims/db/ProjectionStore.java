package com.example.claims.db;

import com.example.claims.contracts.ProjectionRecord;

public interface ProjectionStore {
  /** Inserts the row unless its key (claimId:stageVersion:step) already exists. Returns true if inserted. */
  boolean insertIfAbsent(ProjectionRecord record);

  static String key(ProjectionRecord r) {
    return r.getClaimId() + ":" + r.getStageVersion() + ":" + r.getStep();
  }
}
