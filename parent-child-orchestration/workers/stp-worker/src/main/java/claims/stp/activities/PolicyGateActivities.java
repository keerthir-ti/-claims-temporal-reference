package claims.stp.activities;

import claims.contracts.CoverageResult;
import claims.contracts.StepRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Policy Gate: coverage check. Read only. */
@ActivityInterface
public interface PolicyGateActivities {
  @ActivityMethod(name = "PolicyGate.verifyCoverage")
  CoverageResult verifyCoverage(StepRequest request);
}
