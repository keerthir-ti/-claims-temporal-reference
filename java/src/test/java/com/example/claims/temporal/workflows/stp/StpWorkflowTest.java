package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.TestEnvs;
import com.example.claims.db.InMemoryProjectionStore;
import com.example.claims.temporal.Names;
import com.example.claims.temporal.activities.ClaimSnapshot;
import com.example.claims.temporal.activities.ProjectionActivitiesImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.ClaimEventType;
import com.example.claims.contracts.LifecycleSettings;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackOutcome;
import com.example.claims.contracts.TrackRequest;
import com.example.claims.contracts.TrackResult;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StpWorkflowTest {
  private TestWorkflowEnvironment env;
  private WorkflowClient client;
  private FakeStpActivities activities;
  private InMemoryProjectionStore projection;

  @BeforeEach
  void setUp() {
    env = TestEnvs.create();
    activities = new FakeStpActivities();
    projection = new InMemoryProjectionStore();
    Worker stp = env.newWorker(Names.STP_QUEUE);
    stp.registerWorkflowImplementationTypes(StpWorkflowImpl.class, VendorDispatchWorkflowImpl.class);
    stp.registerActivitiesImplementations(activities);
    env.newWorker(Names.PROJECTION_QUEUE).registerActivitiesImplementations(new ProjectionActivitiesImpl(projection));
    env.start();
    client = env.getWorkflowClient();
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  private WorkflowStub start(LifecycleSettings settings) {
    WorkflowStub stub = client.newUntypedWorkflowStub(Names.STP_WORKFLOW, WorkflowOptions.newBuilder()
        .setWorkflowId("claim:C:stp:" + UUID.randomUUID()).setTaskQueue(Names.STP_QUEUE).build());
    stub.start(new TrackRequest().withSchemaVersion(1).withClaimId("C").withTrack(Track.STP).withStageVersion(4)
        .withSettings(settings));
    return stub;
  }

  private static ClaimEvent event(ClaimEventType type) {
    return new ClaimEvent().withSchemaVersion(1).withEventId(UUID.randomUUID().toString()).withClaimId("C").withEventType(type);
  }

  @Test
  void happyPathRunsTheWholeFlow() {
    TrackResult r = start(new LifecycleSettings()).getResult(TrackResult.class);
    assertEquals(TrackOutcome.COMPLETED, r.getOutcome());
    assertEquals(4, r.getStageVersion());
    assertTrue(activities.calls.contains("OFFER:V-1") && activities.calls.contains("OFFER:V-2"), "grandchild tried both vendors");
    assertEquals(1, activities.calls.stream().filter("ISSUE_PAYMENT"::equals).count());
    assertTrue(projection.rows.containsKey("C:4:ISSUE_PAYMENT"));
  }

  @Test
  void backstopPollFindsEstimateWithoutAnyEvent() {
    activities.estimateReadyAfterChecks = 3;
    TrackResult r = start(new LifecycleSettings().withEventWaitSeconds(3600).withRevalidateIntervalSeconds(60))
        .getResult(TrackResult.class);
    assertEquals(TrackOutcome.COMPLETED, r.getOutcome());
    assertTrue(activities.estimateChecks.get() >= 3);
  }

  @Test
  void materialChangeDuringEstimateWaitRequestsRetriage() {
    activities.estimateReadyAfterChecks = Integer.MAX_VALUE; // estimate never arrives
    WorkflowStub stub = start(new LifecycleSettings().withEventWaitSeconds(3600).withRevalidateIntervalSeconds(600));
    env.sleep(Duration.ofMinutes(5)); // STP is now waiting for the estimate
    activities.claim.set(new ClaimSnapshot(
        "C", 2, "AUTO", "MA", "PLY", "AGENT", "X", true, 1000, true, "CONFIRMED", "OPEN"));
    stub.signal(Names.SIGNAL_CLAIM_EVENT, event(ClaimEventType.CLAIM_UPDATED));
    TrackResult r = stub.getResult(TrackResult.class);
    assertEquals(TrackOutcome.RETRIAGE_REQUESTED, r.getOutcome());
    assertEquals("injury reported", r.getReason());
    assertFalse(activities.calls.contains("ISSUE_PAYMENT"));
  }

  @Test
  void cancelRequestBetweenIssueAndFinalizeCancelsPayment() {
    activities.onIssuePayment = ctx -> client.newUntypedWorkflowStub(
        io.temporal.activity.Activity.getExecutionContext().getInfo().getWorkflowId())
        .signal(Names.SIGNAL_CLAIM_EVENT, event(ClaimEventType.PAYMENT_CANCEL_REQUESTED));
    TrackResult r = start(new LifecycleSettings()).getResult(TrackResult.class);
    assertEquals(TrackOutcome.ADJUSTER_HANDOFF, r.getOutcome());
    assertTrue(activities.calls.contains("CANCEL_PAYMENT"));
    assertFalse(activities.calls.contains("FINALIZE_PAYMENT"));
  }

  @Test
  void staleStageWriteHandsOffInsteadOfRetrying() {
    activities.staleStageOnReserve = true;
    TrackResult r = start(new LifecycleSettings()).getResult(TrackResult.class);
    assertEquals(TrackOutcome.ADJUSTER_HANDOFF, r.getOutcome());
    assertEquals(1, activities.calls.stream().filter("OPEN_RESERVE"::equals).count(), "non-retryable: exactly one attempt");
  }
}
