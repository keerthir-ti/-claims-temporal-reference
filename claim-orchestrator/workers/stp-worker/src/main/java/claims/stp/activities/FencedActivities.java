package claims.stp.activities;

import claims.contracts.v1.AppraisalResult;
import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.ClaimSnapshot;
import claims.contracts.v1.CloseClaimRequest;
import claims.contracts.v1.CloseTaskRequest;
import claims.contracts.v1.ContactResult;
import claims.contracts.v1.CreatePaymentRequest;
import claims.contracts.v1.DispatchResult;
import claims.contracts.v1.Done;
import claims.contracts.v1.EstimateResult;
import claims.contracts.v1.PaymentKeyRef;
import claims.contracts.v1.PaymentRecord;
import claims.contracts.v1.ReserveRequest;
import claims.contracts.v1.ReserveResult;
import claims.contracts.v1.StepRequest;
import claims.contracts.v1.TaskRef;
import claims.contracts.v1.TaskRequest;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.fence.StageFence;

/**
 * Wraps the system activities so every write is refused when the claim has moved to a newer stage
 * version. The worker applies it around any implementation, mock or real, so real implementations do
 * not repeat the check. Closing a task or an already created payment is not fenced: finishing what was
 * started is always allowed.
 */
public final class FencedActivities {
  private FencedActivities() {}

  public static ClaimsProActivities claimsPro(ClaimsProActivities inner, StageFence fence) {
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

  public static CommHubActivities commHub(CommHubActivities inner, StageFence fence) {
    return new CommHubActivities() {
      @Override public ContactResult contactCustomer(StepRequest r) { fence.check(r.getStep()); return inner.contactCustomer(r); }
    };
  }

  public static EClaimsActivities eClaims(EClaimsActivities inner, StageFence fence) {
    return new EClaimsActivities() {
      @Override public DispatchResult dispatchVendor(StepRequest r) { fence.check(r.getStep()); return inner.dispatchVendor(r); }
      @Override public AppraisalResult assignAppraisal(StepRequest r) { fence.check(r.getStep()); return inner.assignAppraisal(r); }
      @Override public EstimateResult getEstimate(ClaimRef ref) { return inner.getEstimate(ref); }
    };
  }
}
