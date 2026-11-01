package claims.runtime.activities;

import claims.contracts.ClaimSnapshot;
import claims.contracts.RiskAssessment;
import claims.contracts.TriageInput;
import claims.contracts.TriageRuling;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Triage work, implemented by the TSA worker (Python) on the claims-tsa queue. */
@ActivityInterface
public interface TriageActivities {
  /** The triage AI agent's risk assessment of the claim. */
  @ActivityMethod(name = "Ai.assessRisk")
  RiskAssessment assessRisk(ClaimSnapshot claim);

  /** The triage rules (config/triage-rules.yaml) applied to the claim and the assessment. */
  @ActivityMethod(name = "Triage.applyRules")
  TriageRuling applyRules(TriageInput input);
}
