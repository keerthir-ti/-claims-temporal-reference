package claims.stp.workflows;

import claims.contracts.ClaimSnapshot;
import java.util.ArrayList;
import java.util.List;

/**
 * Whether a re-read claim differs enough from what STP started with to need re-triage. Pure function,
 * called in workflow code after the fresh snapshot came back from an activity. Cosmetic changes (a new
 * change version with nothing below) cause no churn.
 */
public final class MaterialChange {
  private MaterialChange() {}

  /** @return the reason, or null when the change is not material */
  public static String detect(ClaimSnapshot before, ClaimSnapshot after) {
    List<String> reasons = new ArrayList<>();
    if (!before.getInjuryReported() && after.getInjuryReported()) {
      reasons.add("injury reported");
    }
    if (!before.getLineOfBusiness().equals(after.getLineOfBusiness())) {
      reasons.add("line of business changed");
    }
    if (!before.getState().equals(after.getState())) {
      reasons.add("loss state changed");
    }
    if (after.getClaimedAmountCents() > before.getClaimedAmountCents() * 3 / 2) {
      reasons.add("claimed amount up by more than half");
    }
    if ("CLOSED".equals(after.getStatus())) {
      reasons.add("claim closed in ClaimsPro");
    }
    return reasons.isEmpty() ? null : String.join(", ", reasons);
  }
}
