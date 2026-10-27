package com.example.claims.temporal.activities;

import com.example.claims.contracts.Track;
import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface LifecycleActivities {
  /**
   * Tells ClaimsPro the claim entered a new stage. ClaimsPro then rejects (409) writes carrying an
   * older stage version: the lease/fencing token that stops a late activity from a previous track
   * visit from changing the claim.
   */
  void beginStage(String claimId, int stageVersion, Track track);

  /**
   * The stage version ClaimsPro last saw for this claim. A new run (reopen after the previous run
   * completed) continues from it, so stage versions and idempotency keys keep increasing per claim
   * across runs instead of colliding with the previous run's.
   */
  int currentStageVersion(String claimId);
}
