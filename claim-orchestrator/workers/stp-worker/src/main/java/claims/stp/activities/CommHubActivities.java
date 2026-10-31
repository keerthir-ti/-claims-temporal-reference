package claims.stp.activities;

import claims.contracts.v1.ContactResult;
import claims.contracts.v1.StepRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Comm Hub: customer contact. Idempotency key claimId:stageVersion:step. */
@ActivityInterface
public interface CommHubActivities {
  @ActivityMethod(name = "CommHub.contactCustomer")
  ContactResult contactCustomer(StepRequest request);
}
