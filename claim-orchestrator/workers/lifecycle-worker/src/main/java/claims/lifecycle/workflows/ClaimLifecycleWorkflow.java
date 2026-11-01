package claims.lifecycle.workflows;

import claims.contracts.ClaimEvent;
import claims.contracts.ClaimStart;
import claims.contracts.Decision;
import claims.contracts.DecisionResult;
import claims.contracts.PaymentResult;
import claims.runtime.Names;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** One per claim (workflow ID claim:{claimId}); the only workflow that changes the claim's stage. */
@WorkflowInterface
public interface ClaimLifecycleWorkflow {
  @WorkflowMethod(name = Names.CLAIM_LIFECYCLE)
  void run(ClaimStart start);

  /** Change events from the event bridge; forwarded to the active track or held until one starts. */
  @SignalMethod(name = Names.SIGNAL_CLAIM_EVENT)
  void claimEvent(ClaimEvent event);

  /** Sent by PaymentWorkflow, which keeps running even if STP stops. */
  @SignalMethod(name = Names.SIGNAL_PAYMENT_RESULT)
  void paymentResult(PaymentResult result);

  @UpdateMethod(name = Names.UPDATE_DECIDE)
  DecisionResult decide(Decision decision);

  @UpdateValidatorMethod(updateName = Names.UPDATE_DECIDE)
  void validateDecide(Decision decision);
}
