package claims.stp.activities;

import claims.contracts.ContactResult;
import claims.contracts.StepRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Comm Hub: customer contact. Idempotency key claimId:stageVersion:step. */
@ActivityInterface
public interface CommHubActivities {
  @ActivityMethod(name = "CommHub.contactCustomer")
  ContactResult contactCustomer(StepRequest request);
}
