package claims.lifecycle.workflows;

import claims.contracts.ClaimSnapshot;
import claims.contracts.CoverageAnalysis;
import claims.contracts.DecisionKind;
import claims.contracts.DecisionResult;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackResult;
import java.util.List;

/**
 * Investigation, inside the claim workflow: the AI coverage analysis (activity on claims-is, Python). If facts
 * are missing it waits for documents (an event) or an IS reviewer, then returns the claim to triage.
 */
final class InvestigationModule {
  private final ClaimContext c;

  InvestigationModule(ClaimContext c) {
    this.c = c;
  }

  TrackResult run() {
    for (int attempt = 1; ; attempt++) {
      c.documentsReceived = false;
      ClaimSnapshot claim = c.steps.run("is.read", () -> c.claimsPro.readClaim(c.ref()));
      CoverageAnalysis a = c.steps.run("is.coverage", () -> c.investigation.analyzeCoverage(claim));
      c.recorder.row(c.stage(), "is.analysis." + attempt).outcome(a.getInformationComplete() ? "COMPLETE" : "MISSING")
          .reason(a.getMissing()).write();
      if (a.getInformationComplete()) {
        return result(TrackOutcome.RETRIAGE, "coverage facts gathered");
      }
      DecisionResult d = c.desk.decide(DecisionKind.COVERAGE_INFORMATION, List.of("INFORMATION_RECEIVED", "MANUAL"),
          a.getMissing(), 0, "", () -> c.documentsReceived);
      if (d == null) {
        continue; // documents arrived: analyse again
      }
      return "INFORMATION_RECEIVED".equals(d.getChoice())
          ? result(TrackOutcome.RETRIAGE, "information confirmed by " + d.getDecidedBy())
          : result(TrackOutcome.ROUTE_MANUAL, "coverage information not obtained");
    }
  }

  private static TrackResult result(TrackOutcome o, String reason) {
    return TrackResult.newBuilder().setOutcome(o).setReason(reason).build();
  }
}
