package claims.runtime.client;

import claims.contracts.v1.ClaimEvent;
import claims.contracts.v1.ClaimEventType;
import claims.contracts.v1.ClaimStart;
import claims.runtime.Names;
import claims.runtime.SearchAttrs;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.SearchAttributes;

/**
 * For the existing event bridge: turns one allowlisted, non-personal change event into the Temporal call.
 * Created and reopened claims use Signal-With-Start on claim:{claimId}; every other event is a signal.
 * The bridge advances its read position only after this returns; {@link Result#NO_RUNNING_WORKFLOW}
 * means the event belongs in its dead-letter store. Other exceptions mean "retry".
 */
public final class ClaimEventsClient {
  public enum Result { STARTED_OR_SIGNALLED, SIGNALLED, NO_RUNNING_WORKFLOW }

  private final WorkflowClient client;

  public ClaimEventsClient(WorkflowClient client) {
    this.client = client;
  }

  public Result publish(ClaimEvent event) {
    String workflowId = Names.claimWorkflowId(event.getClaimId());
    boolean starts = event.getType() == ClaimEventType.CLAIM_CREATED || event.getType() == ClaimEventType.CLAIM_REOPENED;
    if (starts) {
      WorkflowStub stub = client.newUntypedWorkflowStub(Names.CLAIM_LIFECYCLE, WorkflowOptions.newBuilder()
          .setWorkflowId(workflowId)
          .setTaskQueue(Names.LIFECYCLE_QUEUE)
          .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
          .setTypedSearchAttributes(SearchAttributes.newBuilder().set(SearchAttrs.CLAIM_ID, event.getClaimId()).build())
          .build());
      ClaimStart start = ClaimStart.newBuilder()
          .setClaimId(event.getClaimId())
          .setReopened(event.getType() == ClaimEventType.CLAIM_REOPENED)
          .setStartEventId(event.getEventId())
          .build();
      stub.signalWithStart(Names.SIGNAL_CLAIM_EVENT, new Object[] {event}, new Object[] {start});
      return Result.STARTED_OR_SIGNALLED;
    }
    try {
      client.newUntypedWorkflowStub(workflowId).signal(Names.SIGNAL_CLAIM_EVENT, event);
      return Result.SIGNALLED;
    } catch (WorkflowNotFoundException e) {
      return Result.NO_RUNNING_WORKFLOW;
    }
  }
}
