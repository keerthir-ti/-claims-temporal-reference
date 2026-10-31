package claims.stp.activities;

import claims.contracts.v1.ClaimSnapshot;
import claims.contracts.v1.FlowDefinition;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Picks the validated STP flow definition for the claim's line of business, company, channel and state. */
@ActivityInterface
public interface FlowActivities {
  @ActivityMethod(name = "Flows.resolve")
  FlowDefinition resolve(ClaimSnapshot claim);
}
