package claims.runtime.activities;

import claims.contracts.ClaimRef;
import claims.contracts.ClaimSnapshot;
import claims.contracts.CloseClaimRequest;
import claims.contracts.CloseTaskRequest;
import claims.contracts.CreatePaymentRequest;
import claims.contracts.Done;
import claims.contracts.PaymentKeyRef;
import claims.contracts.PaymentRecord;
import claims.contracts.ReserveRequest;
import claims.contracts.ReserveResult;
import claims.contracts.TaskRef;
import claims.contracts.TaskRequest;
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
