package claims.stp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import claims.contracts.ClaimEvent;
import claims.contracts.ClaimEventType;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackRequest;
import claims.contracts.TrackResult;
import claims.runtime.Names;
import claims.stp.workflows.StpWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import org.junit.jupiter.api.Test;

class StpWorkflowTest {
  private static StpWorkflow start(StpTestEnv t) {
    return start(t, 2);
  }

  private static StpWorkflow start(StpTestEnv t, int stageVersion) {
    StpWorkflow wf = t.env.getWorkflowClient().newWorkflowStub(StpWorkflow.class,
        WorkflowOptions.newBuilder().setWorkflowId("claim:C1:stp:" + stageVersion).setTaskQueue(Names.STP_QUEUE).build());
    WorkflowClient.start(wf::run, TrackRequest.newBuilder().setClaimId("C1").setStageVersion(stageVersion).build());
    return wf;
  }

  @Test
  void runsTheFlowPaysAndCloses() {
    try (StpTestEnv t = new StpTestEnv()) {
      t.systems.claim("C1", 400_000);
      TrackResult result = WorkflowStub.fromTyped(start(t)).getResult(TrackResult.class);
      assertEquals(TrackOutcome.COMPLETED, result.getOutcome());
      assertEquals(1, t.systems.paymentCreates.get());
      assertEquals(1, t.systems.closedClaims.size());
      assertEquals("test@1#x", result.getFlowRef());
    }
  }

  @Test
  void materialChangeWhileWaitingEndsWithRetriage() throws Exception {
    try (StpTestEnv t = new StpTestEnv()) {
      t.systems.claim("C1", 400_000);
      t.stp.estimateAvailable = false;
      StpWorkflow wf = start(t);
      while (t.systems.rows.stream().noneMatch(r -> r.getStep().equals("stp.dispatch.1"))) {
        Thread.sleep(25);
      }
      t.systems.claims.computeIfPresent("C1", (k, c) -> c.toBuilder().setChangeVersion(2).setInjuryReported(true).build());
      wf.claimEvent(ClaimEvent.newBuilder().setEventId("e1").setClaimId("C1").setType(ClaimEventType.CLAIM_UPDATED).build());
      TrackResult result = WorkflowStub.fromTyped(wf).getResult(TrackResult.class);
      assertEquals(TrackOutcome.RETRIAGE, result.getOutcome());
      assertEquals(0, t.systems.paymentCreates.get());
    }
  }

  @Test
  void laterVisitOfAPaidClaimDoesNotPayAgain() {
    try (StpTestEnv t = new StpTestEnv()) {
      t.systems.claim("C1", 400_000);
      assertEquals(TrackOutcome.COMPLETED, WorkflowStub.fromTyped(start(t, 2)).getResult(TrackResult.class).getOutcome());
      // e.g. the claim came back to STP from manual handling after the close failed
      TrackResult second = WorkflowStub.fromTyped(start(t, 4)).getResult(TrackResult.class);
      assertEquals(TrackOutcome.COMPLETED, second.getOutcome());
      assertEquals(1, t.systems.paymentCreates.get());
      assertEquals(1, t.systems.rows.stream()
          .filter(r -> r.getStep().equals("stp.payment") && r.getOutcome().equals("ALREADY_PAID")).count());
    }
  }
}
