package claims.stp.activities;

import claims.contracts.AppraisalResult;
import claims.contracts.ClaimRef;
import claims.contracts.DispatchResult;
import claims.contracts.EstimateResult;
import claims.contracts.StepRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** eClaims: vendor dispatch, appraisal, estimate. Writes use the idempotency key claimId:stageVersion:step. */
@ActivityInterface
public interface EClaimsActivities {
  @ActivityMethod(name = "EClaims.dispatchVendor")
  DispatchResult dispatchVendor(StepRequest request);

  @ActivityMethod(name = "EClaims.assignAppraisal")
  AppraisalResult assignAppraisal(StepRequest request);

  @ActivityMethod(name = "EClaims.getEstimate")
  EstimateResult getEstimate(ClaimRef ref);
}
