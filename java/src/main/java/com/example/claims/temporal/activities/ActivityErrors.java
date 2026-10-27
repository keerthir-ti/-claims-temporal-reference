package com.example.claims.temporal.activities;

/**
 * Non-retryable failure types shared by every adapter, in Java and Python. Workflows list them in
 * their RetryOptions; anything else (5xx, timeouts, connection errors) is retried by Temporal.
 */
public final class ActivityErrors {
  private ActivityErrors() {}

  /** 409: the claim has moved to a newer stage; this write comes from an abandoned stage. */
  public static final String STALE_STAGE = "StaleStage";
  /** 404 */
  public static final String NOT_FOUND = "ClaimNotFound";
  /** 422: the system refused the request; retrying will not help. */
  public static final String BUSINESS_RULE = "BusinessRuleViolation";
}
