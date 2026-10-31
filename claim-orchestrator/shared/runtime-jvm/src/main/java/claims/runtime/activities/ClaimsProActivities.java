package claims.runtime.activities;

import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.ClaimSnapshot;
import claims.contracts.v1.CloseClaimRequest;
import claims.contracts.v1.CloseTaskRequest;
import claims.contracts.v1.CreatePaymentRequest;
import claims.contracts.v1.Done;
import claims.contracts.v1.PaymentKeyRef;
import claims.contracts.v1.PaymentRecord;
import claims.contracts.v1.ReserveRequest;
import claims.contracts.v1.ReserveResult;
import claims.contracts.v1.TaskRef;
import claims.contracts.v1.TaskRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Every ClaimsPro call. Runs on the rate-limited claimspro-activities queue, called from Java and
 * Python workflows by these names.
 *
 * <p>Implementations: read current data by reference; use claimId:stageVersion:step as the
 * idempotency key so a repeat returns the first result; createPayment looks up the payment key
 * before creating. The stage-version check is added by the worker around the implementation.
 */
@ActivityInterface
public interface ClaimsProActivities {
  @ActivityMethod(name = "ClaimsPro.readClaim")
  ClaimSnapshot readClaim(ClaimRef ref);

  @ActivityMethod(name = "ClaimsPro.createTask")
  TaskRef createTask(TaskRequest request);

  @ActivityMethod(name = "ClaimsPro.closeTask")
  Done closeTask(CloseTaskRequest request);

  @ActivityMethod(name = "ClaimsPro.openReserve")
  ReserveResult openReserve(ReserveRequest request);

  @ActivityMethod(name = "ClaimsPro.createPayment")
  PaymentRecord createPayment(CreatePaymentRequest request);

  @ActivityMethod(name = "ClaimsPro.closePayment")
  PaymentRecord closePayment(PaymentKeyRef request);

  @ActivityMethod(name = "ClaimsPro.cancelPayment")
  PaymentRecord cancelPayment(PaymentKeyRef request);

  @ActivityMethod(name = "ClaimsPro.closeClaim")
  Done closeClaim(CloseClaimRequest request);
}
