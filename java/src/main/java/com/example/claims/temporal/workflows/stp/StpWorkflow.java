package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.Names;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.TrackRequest;
import com.example.claims.contracts.TrackResult;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.List;

@WorkflowInterface
public interface StpWorkflow {
  @WorkflowMethod(name = Names.STP_WORKFLOW)
  TrackResult run(TrackRequest request);

  /** Events forwarded by the parent. */
  @SignalMethod(name = Names.SIGNAL_CLAIM_EVENT)
  void claimEvent(ClaimEvent event);

  @QueryMethod(name = "getProgress")
  Progress getProgress();

  record Progress(String flow, String stage, List<String> completedSteps, String waitingFor, String retriageReason) {}
}
