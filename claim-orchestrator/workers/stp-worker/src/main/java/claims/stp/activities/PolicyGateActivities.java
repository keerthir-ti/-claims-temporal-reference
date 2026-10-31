package claims.stp.activities;

import claims.contracts.v1.CoverageResult;
import claims.contracts.v1.StepRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Policy Gate: coverage check. Read only. */
@ActivityInterface
public interface PolicyGateActivities {
  @ActivityMethod(name = "PolicyGate.verifyCoverage")
  CoverageResult verifyCoverage(StepRequest request);
}
