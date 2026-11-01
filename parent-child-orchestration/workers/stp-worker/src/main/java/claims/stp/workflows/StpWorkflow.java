package claims.stp.workflows;

import claims.contracts.ClaimEvent;
import claims.contracts.Decision;
import claims.contracts.DecisionResult;
import claims.contracts.TrackRequest;
import claims.contracts.TrackResult;
import claims.runtime.Names;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** One STP visit (claim:{claimId}:stp:{stage}). Returns an outcome; never moves the claim itself. */
@WorkflowInterface
public interface StpWorkflow {
  @WorkflowMethod(name = Names.STP)
  TrackResult run(TrackRequest request);

  @SignalMethod(name = Names.SIGNAL_CLAIM_EVENT)
  void claimEvent(ClaimEvent event);

  @UpdateMethod(name = Names.UPDATE_DECIDE)
  DecisionResult decide(Decision decision);

  @UpdateValidatorMethod(updateName = Names.UPDATE_DECIDE)
  void validateDecide(Decision decision);
}
