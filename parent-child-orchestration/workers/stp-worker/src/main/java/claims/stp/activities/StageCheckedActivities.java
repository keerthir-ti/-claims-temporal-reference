package claims.stp.activities;

import claims.contracts.AppraisalResult;
import claims.contracts.ClaimRef;
import claims.contracts.ClaimSnapshot;
import claims.contracts.CloseClaimRequest;
import claims.contracts.CloseTaskRequest;
import claims.contracts.ContactResult;
import claims.contracts.CreatePaymentRequest;
import claims.contracts.DispatchResult;
import claims.contracts.Done;
import claims.contracts.EstimateResult;
import claims.contracts.PaymentKeyRef;
import claims.contracts.PaymentRecord;
import claims.contracts.ReserveRequest;
import claims.contracts.ReserveResult;
import claims.contracts.StepRequest;
import claims.contracts.TaskRef;
import claims.contracts.TaskRequest;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.stage.StageVersionCheck;

/**
 * Wraps the system activities so every write is refused when the claim has moved to a newer stage
 * version. The worker applies it around any implementation, mock or real, so real implementations do
 * not repeat the check. Closing a task or an already created payment is not fenced: finishing what was
 * started is always allowed.
 */
public final class StageCheckedActivities {
  private StageCheckedActivities() {}

  public static ClaimsProActivities claimsPro(ClaimsProActivities inner, StageVersionCheck fence) {
    return new ClaimsProActivities() {
      @Override public ClaimSnapshot readClaim(ClaimRef ref) { return inner.readClaim(ref); }
      @Override public TaskRef createTask(TaskRequest r) { fence.check(r.getStep()); return inner.createTask(r); }
      @Override public Done closeTask(CloseTaskRequest r) { return inner.closeTask(r); }
      @Override public ReserveResult openReserve(ReserveRequest r) { fence.check(r.getStep()); return inner.openReserve(r); }
      @Override public PaymentRecord createPayment(CreatePaymentRequest r) { fence.check(r.getStep()); return inner.createPayment(r); }
      @Override public PaymentRecord closePayment(PaymentKeyRef r) { return inner.closePayment(r); }
      @Override public PaymentRecord cancelPayment(PaymentKeyRef r) { return inner.cancelPayment(r); }
      @Override public Done closeClaim(CloseClaimRequest r) { fence.check(r.getStep()); return inner.closeClaim(r); }
    };
  }

  public static CommHubActivities commHub(CommHubActivities inner, StageVersionCheck fence) {
    return new CommHubActivities() {
      @Override public ContactResult contactCustomer(StepRequest r) { fence.check(r.getStep()); return inner.contactCustomer(r); }
    };
  }

  public static EClaimsActivities eClaims(EClaimsActivities inner, StageVersionCheck fence) {
    return new EClaimsActivities() {
      @Override public DispatchResult dispatchVendor(StepRequest r) { fence.check(r.getStep()); return inner.dispatchVendor(r); }
      @Override public AppraisalResult assignAppraisal(StepRequest r) { fence.check(r.getStep()); return inner.assignAppraisal(r); }
      @Override public EstimateResult getEstimate(ClaimRef ref) { return inner.getEstimate(ref); }
    };
  }
}
