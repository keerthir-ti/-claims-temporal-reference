package com.example.claims.temporal.activities;

/** Outcome of one STP step. status: OK | HANDOFF | RETRIAGE | CANCELLED. */
public record StepResult(String status, String detail, String ref, Long amountCents) {
  public static StepResult ok(String detail) {
    return new StepResult("OK", detail, null, null);
  }

  public static StepResult ok(String detail, String ref, Long amountCents) {
    return new StepResult("OK", detail, ref, amountCents);
  }

  public static StepResult handoff(String detail) {
    return new StepResult("HANDOFF", detail, null, null);
  }

  public static StepResult retriage(String detail) {
    return new StepResult("RETRIAGE", detail, null, null);
  }

  public boolean isOk() {
    return "OK".equals(status);
  }
}
