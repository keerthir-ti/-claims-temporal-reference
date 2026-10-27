package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.activities.ClaimSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Decides whether a re-read claim differs enough from what STP started with to need re-triage.
 * Pure function: called from workflow code after the fresh snapshot came back from an activity.
 */
public final class MaterialChange {
  private MaterialChange() {}

  /** @return a reason, or null when the change is not material */
  public static String detect(ClaimSnapshot before, ClaimSnapshot after) {
    List<String> reasons = new ArrayList<>();
    if (!Objects.equals(before.lob(), after.lob())) {
      reasons.add("line of business changed");
    }
    if (!Objects.equals(before.lossType(), after.lossType())) {
      reasons.add("loss type changed");
    }
    if (!before.injuryReported() && after.injuryReported()) {
      reasons.add("injury reported");
    }
    if (before.policyActive() && !after.policyActive()) {
      reasons.add("policy no longer active");
    }
    if (!Objects.equals(before.coverageStatus(), after.coverageStatus())
        && !"CONFIRMED".equals(after.coverageStatus())) {
      reasons.add("coverage now " + after.coverageStatus());
    }
    if (after.estimatedLossCents() > before.estimatedLossCents() * 3 / 2) {
      reasons.add("estimated loss up more than 50%");
    }
    return reasons.isEmpty() ? null : String.join(", ", reasons);
  }
}
