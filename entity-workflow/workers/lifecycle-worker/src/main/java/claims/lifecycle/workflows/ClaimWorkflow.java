package claims.lifecycle.workflows;

import claims.contracts.CancelPayment;
import claims.contracts.ClaimEvent;
import claims.contracts.ClaimStart;
import claims.contracts.Decision;
import claims.contracts.DecisionResult;
import claims.runtime.Names;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * The claim: one workflow per claim (workflow ID claim:{claimId}) for its whole life. Triage, investigation,
 * STP and payment run inside it; their work runs as activities on each track's own task queue. Events,
 * decisions and payment cancellation all arrive here.
 */
@WorkflowInterface
public interface ClaimWorkflow {
  @WorkflowMethod(name = Names.CLAIM)
  void run(ClaimStart start);

  /** Change events from the event bridge. */
  @SignalMethod(name = Names.SIGNAL_CLAIM_EVENT)
  void claimEvent(ClaimEvent event);

  /** Cancel the claim's payment; honoured until the payment closes. */
  @SignalMethod(name = Names.SIGNAL_CANCEL_PAYMENT)
  void cancelPayment(CancelPayment cancel);

  /** Every adjuster decision of the claim. */
  @UpdateMethod(name = Names.UPDATE_DECIDE)
  DecisionResult decide(Decision decision);

  @UpdateValidatorMethod(updateName = Names.UPDATE_DECIDE)
  void validateDecide(Decision decision);
}
