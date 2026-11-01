package claims.lifecycle.workflows;

import claims.contracts.ClaimSnapshot;
import claims.contracts.DecisionKind;
import claims.contracts.DecisionResult;
import claims.contracts.RiskAssessment;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackResult;
import claims.contracts.TriageInput;
import claims.contracts.TriageRuling;
import claims.runtime.workflow.DecisionDesk;
import java.util.List;
import java.util.Map;

/**
 * Triage, inside the claim workflow: read the claim, the AI risk assessment and the triage rules (activities
 * on claims-tsa, Python). If the claim changes while triage runs, it is assessed again (up to three passes).
 * Low AI confidence goes to a triage adjuster. Returns a route; the stage machine applies it.
 */
final class TriageModule {
  private static final int MAX_PASSES = 3;
  private static final Map<String, TrackOutcome> ROUTES = Map.of(
      "STP", TrackOutcome.ROUTE_STP, "INVESTIGATION", TrackOutcome.ROUTE_INVESTIGATION,
      "MANUAL", TrackOutcome.ROUTE_MANUAL, "SIU", TrackOutcome.ROUTE_SIU);

  private final ClaimContext c;

  TriageModule(ClaimContext c) {
    this.c = c;
  }

  TrackResult run() {
    TriageRuling ruling = null;
    RiskAssessment risk = null;
    int pass = 0;
    while (pass < MAX_PASSES) {
      pass++;
      c.claimChanged = false;
      ClaimSnapshot claim = c.steps.run("triage.read", () -> c.claimsPro.readClaim(c.ref()));
      RiskAssessment r = c.steps.run("triage.risk", () -> c.triage.assessRisk(claim));
      risk = r;
      ruling = c.triage.applyRules(TriageInput.newBuilder().setClaim(claim).setRisk(r)
          .setConfidenceThreshold(c.policy.getTriageConfidenceThreshold()).build());
      if (!c.claimChanged) {
        break;
      }
    }
    c.recorder.row(c.stage(), "triage.ruling").outcome(ruling.getRoute()).reason(ruling.getReason())
        .ref("fraud_score", String.format("%.2f", risk.getFraudScore()))
        .ref("confidence", String.format("%.2f", risk.getConfidence())).ref("passes", Integer.toString(pass)).write();
    String route = ruling.getRoute();
    String reason = ruling.getReason();
    if (ruling.getNeedsReview()) {
      DecisionResult d = c.desk.decide(DecisionKind.TRIAGE_REVIEW, List.copyOf(ROUTES.keySet()).stream().sorted().toList(),
          "AI confidence " + String.format("%.2f", risk.getConfidence()) + "; suggested " + route + ": " + reason,
          0, "", () -> false);
      if (d == null || DecisionDesk.DEADLINE_PASSED.equals(d.getChoice())) {
        route = "MANUAL";
        reason = "triage review not completed";
      } else {
        route = d.getChoice();
        reason = "chosen by " + d.getDecidedBy() + " on triage review";
      }
    }
    return TrackResult.newBuilder().setOutcome(ROUTES.getOrDefault(route, TrackOutcome.ROUTE_MANUAL)).setReason(reason).build();
  }
}
