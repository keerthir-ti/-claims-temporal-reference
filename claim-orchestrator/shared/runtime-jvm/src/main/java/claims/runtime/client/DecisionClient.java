package claims.runtime.client;

import claims.contracts.v1.Decision;
import claims.contracts.v1.DecisionResult;
import claims.runtime.Names;
import io.temporal.client.UpdateOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowUpdateStage;
import io.temporal.client.WorkflowStub;

/**
 * For the service behind Kong that receives an adjuster's decision. The task in ClaimsPro carries the
 * workflow ID and decision ID; the decision ID is the Update ID, so a retry returns the first result.
 * A rejected decision throws with the validator's reason.
 */
public final class DecisionClient {
  private final WorkflowClient client;

  public DecisionClient(WorkflowClient client) {
    this.client = client;
  }

  public DecisionResult decide(String workflowId, Decision decision) {
    WorkflowStub stub = client.newUntypedWorkflowStub(workflowId);
    UpdateOptions<DecisionResult> options = UpdateOptions.newBuilder(DecisionResult.class)
        .setUpdateName(Names.UPDATE_DECIDE)
        .setUpdateId(decision.getDecisionId())
        .setWaitForStage(WorkflowUpdateStage.COMPLETED)
        .build();
    return stub.startUpdate(options, decision).getResult();
  }
}
