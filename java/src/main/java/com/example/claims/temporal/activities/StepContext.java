package com.example.claims.temporal.activities;

/** Identifies one step execution. Its key doubles as the ClaimsPro idempotency key and the projection key. */
public record StepContext(String claimId, int stageVersion, String step) {
  public String idempotencyKey() {
    return claimId + ":" + stageVersion + ":" + step;
  }
}
