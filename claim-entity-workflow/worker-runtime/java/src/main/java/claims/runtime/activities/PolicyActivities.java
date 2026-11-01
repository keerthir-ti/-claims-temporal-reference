package claims.runtime.activities;

import claims.contracts.DecisionPolicy;
import claims.contracts.PolicyRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Decision roles, deadlines, authority and limits, read once per workflow run and kept in history. */
@ActivityInterface
public interface PolicyActivities {
  @ActivityMethod(name = "Policy.load")
  DecisionPolicy load(PolicyRequest request);
}
