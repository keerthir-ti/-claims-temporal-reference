package claims.runtime.activities;

import claims.contracts.v1.DecisionPolicy;
import claims.contracts.v1.PolicyRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Decision roles, deadlines, authority and limits, read once per workflow run and kept in history. */
@ActivityInterface
public interface PolicyActivities {
  @ActivityMethod(name = "Policy.load")
  DecisionPolicy load(PolicyRequest request);
}
