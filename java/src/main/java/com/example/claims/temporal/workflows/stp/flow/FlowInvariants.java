package com.example.claims.temporal.workflows.stp.flow;

import java.util.Set;

/**
 * Rules the YAML cannot override. Flows choose ordering and optional steps; money authority and
 * the "never interrupt a payment" rule stay in compiled, code-reviewed Java.
 */
public final class FlowInvariants {
  private FlowInvariants() {}

  /** The registered operations. A flow naming anything else is rejected at load time. */
  public static final Set<String> REGISTERED_STEPS = Set.of(
      "INITIAL_CONTACT", "VERIFY_COVERAGE", "OPEN_RESERVE", "VENDOR_DISPATCH", "ASSIGN_APPRAISAL",
      "AWAIT_ESTIMATE", "ISSUE_PAYMENT", "FINALIZE_PAYMENT", "CLOSE_CLAIM");

  public static final Set<String> PAYMENT_STEPS = Set.of("ISSUE_PAYMENT", "FINALIZE_PAYMENT");

  /** Hard ceiling for straight-through payments regardless of flow config: $25,000. */
  public static final long MAX_AUTHORITY_CENTS = 2_500_000L;

  public static long effectiveAuthority(FlowDefinition flow) {
    return Math.min(flow.authorityLimitCents(), MAX_AUTHORITY_CENTS);
  }

  public static boolean interruptible(FlowDefinition.Stage stage) {
    boolean touchesMoney = stage.steps().stream().anyMatch(PAYMENT_STEPS::contains);
    return !touchesMoney && !Boolean.FALSE.equals(stage.interruptible());
  }
}
