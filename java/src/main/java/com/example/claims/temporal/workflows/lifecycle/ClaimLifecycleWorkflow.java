package com.example.claims.temporal.workflows.lifecycle;

import com.example.claims.temporal.Names;
import com.example.claims.contracts.AdjusterDecision;
import com.example.claims.contracts.AdjusterDecisionAck;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.LifecycleInput;
import com.example.claims.contracts.LifecycleState;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * One execution per claim (Workflow ID {@code claim:{claimId}}). Owns ONLY lifecycle routing:
 * which track is active, the stage version (lease), event fan-in and human decisions. All business
 * work happens in the child tracks, which it knows only by workflow type name, task queue and the
 * TrackRequest/TrackResult contract.
 */
@WorkflowInterface
public interface ClaimLifecycleWorkflow {

  @WorkflowMethod(name = Names.LIFECYCLE_WORKFLOW)
  LifecycleState run(LifecycleInput input);

  /** Single front door for Claims Event Bridge events. De-duplicated by eventId. */
  @SignalMethod(name = Names.SIGNAL_CLAIM_EVENT)
  void claimEvent(ClaimEvent event);

  /** Human-in-the-loop decision. An Update (not a Signal) so the caller gets an ack or a rejection. */
  @UpdateMethod(name = Names.UPDATE_ADJUSTER_DECISION)
  AdjusterDecisionAck submitAdjusterDecision(AdjusterDecision decision);

  @UpdateValidatorMethod(updateName = Names.UPDATE_ADJUSTER_DECISION)
  void validateAdjusterDecision(AdjusterDecision decision);

  @QueryMethod(name = Names.QUERY_STATE)
  LifecycleState getState();
}
