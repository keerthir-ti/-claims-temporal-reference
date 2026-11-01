package claims.runtime.activities;

import claims.contracts.ClaimSnapshot;
import claims.contracts.CoverageAnalysis;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Investigation work, implemented by the IS worker (Python) on the claims-is queue. */
@ActivityInterface
public interface InvestigationActivities {
  /** The coverage AI agent: are the coverage facts complete, and what is missing. */
  @ActivityMethod(name = "Ai.analyzeCoverage")
  CoverageAnalysis analyzeCoverage(ClaimSnapshot claim);
}
