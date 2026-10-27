package com.example.claims.temporal.workflows.lifecycle;

import com.example.claims.temporal.TestEnvs;
import com.example.claims.db.InMemoryProjectionStore;
import com.example.claims.temporal.Names;
import com.example.claims.temporal.activities.LifecycleActivities;
import com.example.claims.temporal.activities.ProjectionActivitiesImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.claims.contracts.AdjusterAction;
import com.example.claims.contracts.AdjusterDecision;
import com.example.claims.contracts.AdjusterDecisionAck;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.ClaimEventType;
import com.example.claims.contracts.LifecycleInput;
import com.example.claims.contracts.LifecycleSettings;
import com.example.claims.contracts.LifecycleState;
import com.example.claims.contracts.Track;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClaimLifecycleWorkflowTest {
  private TestWorkflowEnvironment env;
  private WorkflowClient client;
  private final List<String> stages = new CopyOnWriteArrayList<>();
  /** Holds beginStage for "siu-race" claims entering SIU, to open a window for an early Update. */
  private final CountDownLatch raceGate = new CountDownLatch(1);
  /** Stands in for ClaimsPro's current stage per claim. */
  private final Map<String, Integer> claimStage = new ConcurrentHashMap<>();

  @BeforeEach
  void setUp() {
    env = TestEnvs.create();
    Worker lifecycle = env.newWorker(Names.LIFECYCLE_QUEUE);
    lifecycle.registerWorkflowImplementationTypes(ClaimLifecycleWorkflowImpl.class);
    lifecycle.registerActivitiesImplementations(new LifecycleActivities() {
      @Override
      public void beginStage(String claimId, int v, Track track) {
        stages.add(v + ":" + track);
        claimStage.merge(claimId, v, Math::max);
        if (claimId.startsWith("siu-race") && track == Track.SIU) {
          try {
            raceGate.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }

      @Override
      public int currentStageVersion(String claimId) {
        return claimStage.getOrDefault(claimId, 0);
      }
    });
    env.newWorker(Names.PROJECTION_QUEUE).registerActivitiesImplementations(new ProjectionActivitiesImpl(new InMemoryProjectionStore()));
    env.newWorker(Names.TSA_QUEUE).registerWorkflowImplementationTypes(StubTracks.TsaImpl.class);
    env.newWorker(Names.STP_QUEUE).registerWorkflowImplementationTypes(StubTracks.StpImpl.class);
    env.start();
    client = env.getWorkflowClient();
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  private WorkflowStub start(String claimId, LifecycleSettings settings) {
    return start(claimId, settings, false);
  }

  private WorkflowStub start(String claimId, LifecycleSettings settings, boolean reopened) {
    WorkflowStub stub = client.newUntypedWorkflowStub(Names.LIFECYCLE_WORKFLOW, WorkflowOptions.newBuilder()
        .setWorkflowId(Names.lifecycleWorkflowId(claimId)).setTaskQueue(Names.LIFECYCLE_QUEUE).build());
    stub.start(new LifecycleInput().withSchemaVersion(1).withClaimId(claimId).withSettings(settings).withReopened(reopened));
    return stub;
  }

  private static ClaimEvent event(String claimId, String id, ClaimEventType type) {
    return new ClaimEvent().withSchemaVersion(1).withEventId(id).withClaimId(claimId).withEventType(type);
  }

  private static AdjusterDecision decision(String claimId, String id, AdjusterAction action) {
    return new AdjusterDecision().withSchemaVersion(1).withDecisionId(id).withClaimId(claimId).withAction(action);
  }

  private static List<Track> path(LifecycleState s) {
    return s.getTransitions().stream().map(t -> t.getTo()).toList();
  }

  @Test
  void happyPathClosesThenCompletesAfterReopenWindow() {
    LifecycleState s = start("happy-" + UUID.randomUUID(), new LifecycleSettings()).getResult(LifecycleState.class);
    assertEquals(List.of(Track.TSA, Track.STP, Track.CLOSED), path(s));
    assertEquals(List.of("1:TSA", "2:STP", "3:CLOSED"), stages, "every transition fences ClaimsPro with a new stage version");
  }

  @Test
  void pilotScopeSkipsTsa() {
    LifecycleState s = start("happy-" + UUID.randomUUID(), new LifecycleSettings().withTsaEnabled(false))
        .getResult(LifecycleState.class);
    assertEquals(List.of(Track.STP, Track.CLOSED), path(s));
  }

  @Test
  void retriageLoopIsBoundedThenEscalates() {
    String id = "retriage-99-" + UUID.randomUUID(); // STP always asks for re-triage
    WorkflowStub stub = start(id, new LifecycleSettings().withMaxRetriage(2));
    env.sleep(Duration.ofSeconds(5));
    LifecycleState s = stub.query(Names.QUERY_STATE, LifecycleState.class);
    assertEquals(Track.ESCALATION, s.getCurrentTrack());
    assertEquals(3, s.getRetriageCount());
  }

  @Test
  void eventsAreDeduplicatedAndForwardedToTheActiveChild() {
    String id = "wait-" + UUID.randomUUID();
    WorkflowStub stub = start(id, new LifecycleSettings());
    env.sleep(Duration.ofSeconds(2));
    assertEquals(Track.STP, stub.query(Names.QUERY_STATE, LifecycleState.class).getCurrentTrack());
    stub.signal(Names.SIGNAL_CLAIM_EVENT, event(id, "doc-1", ClaimEventType.DOCUMENT_RECEIVED)); // not relevant to STP: buffered
    stub.signal(Names.SIGNAL_CLAIM_EVENT, event(id, "est-1", ClaimEventType.ESTIMATE_RECEIVED));
    stub.signal(Names.SIGNAL_CLAIM_EVENT, event(id, "est-1", ClaimEventType.ESTIMATE_RECEIVED)); // duplicate
    env.sleep(Duration.ofSeconds(2));
    LifecycleState s = stub.query(Names.QUERY_STATE, LifecycleState.class);
    assertEquals(Track.CLOSED, s.getCurrentTrack(), "estimate was forwarded to STP");
    assertEquals(1, s.getBufferedEventCount());
  }

  @Test
  void adjusterDecisionIsValidatedAndIdempotent() {
    String id = "siu-" + UUID.randomUUID();
    WorkflowStub stub = start(id, new LifecycleSettings());
    env.sleep(Duration.ofSeconds(2));
    assertEquals(Track.SIU, stub.query(Names.QUERY_STATE, LifecycleState.class).getCurrentTrack());

    assertThrows(WorkflowUpdateException.class, () -> stub.update(Names.UPDATE_ADJUSTER_DECISION,
        AdjusterDecisionAck.class, decision("other-claim", "d1", AdjusterAction.CLOSE)));

    AdjusterDecisionAck first = stub.update(Names.UPDATE_ADJUSTER_DECISION, AdjusterDecisionAck.class,
        decision(id, "d1", AdjusterAction.CLOSE));
    AdjusterDecisionAck retry = stub.update(Names.UPDATE_ADJUSTER_DECISION, AdjusterDecisionAck.class,
        decision(id, "d1", AdjusterAction.CLOSE));
    assertFalse(first.getDuplicate());
    assertTrue(retry.getDuplicate());
    assertEquals(Track.CLOSED, stub.getResult(LifecycleState.class).getCurrentTrack());
  }

  @Test
  void decisionAcceptedWhileTheTransitionIsStillRunningIsNotLost() {
    // Race seen on the live stack: the track flips to SIU, then beginStage/projection activities run.
    // An Update accepted in that window must survive into awaitAdjuster().
    String id = "siu-race-" + UUID.randomUUID();
    WorkflowStub stub = start(id, new LifecycleSettings());
    while (!stages.contains("2:SIU")) {
      env.sleep(Duration.ofMillis(100));
    }
    AdjusterDecisionAck ack = stub.update(Names.UPDATE_ADJUSTER_DECISION, AdjusterDecisionAck.class,
        decision(id, "early", AdjusterAction.CLOSE));
    assertEquals(Track.SIU, ack.getTrackAtDecision());
    raceGate.countDown();
    env.sleep(Duration.ofSeconds(5));
    assertEquals(List.of(Track.TSA, Track.SIU, Track.CLOSED), path(stub.query(Names.QUERY_STATE, LifecycleState.class)));
  }

  @Test
  void longWaitInSiuContinuesAsNewAndStillAcceptsTheDecision() {
    String id = "siu-wait-" + UUID.randomUUID();
    WorkflowStub stub = start(id, new LifecycleSettings().withManualSlaSeconds(60).withContinueAsNewEventThreshold(40));
    env.sleep(Duration.ofMinutes(30)); // ~30 SLA breaches while nobody decides
    io.temporal.api.history.v1.HistoryEvent first =
        client.fetchHistory(Names.lifecycleWorkflowId(id)).getEvents().get(0);
    assertFalse(first.getWorkflowExecutionStartedEventAttributes().getContinuedExecutionRunId().isEmpty(),
        "history was compacted with Continue-As-New while waiting");
    WorkflowStub current = client.newUntypedWorkflowStub(Names.lifecycleWorkflowId(id));
    assertEquals(Track.SIU, current.query(Names.QUERY_STATE, LifecycleState.class).getCurrentTrack());
    current.update(Names.UPDATE_ADJUSTER_DECISION, AdjusterDecisionAck.class, decision(id, "late", AdjusterAction.CLOSE));
    env.sleep(Duration.ofSeconds(5));
    assertEquals(List.of(Track.TSA, Track.SIU, Track.CLOSED), path(current.query(Names.QUERY_STATE, LifecycleState.class)));
  }

  @Test
  void manualSlaBreachEscalates() {
    String id = "retriage-99-" + UUID.randomUUID();
    WorkflowStub stub = start(id, new LifecycleSettings().withTsaEnabled(false).withManualSlaSeconds(3600));
    env.sleep(Duration.ofMinutes(61));
    LifecycleState s = stub.query(Names.QUERY_STATE, LifecycleState.class);
    assertEquals(List.of(Track.STP, Track.MANUAL, Track.ESCALATION), path(s));
  }

  @Test
  void reopenInsideWindowGoesBackToTriage() {
    String id = "happy-" + UUID.randomUUID();
    WorkflowStub stub = start(id, new LifecycleSettings().withReopenWindowSeconds(86400));
    env.sleep(Duration.ofSeconds(5));
    stub.signal(Names.SIGNAL_CLAIM_EVENT, event(id, "r-1", ClaimEventType.CLAIM_REOPENED));
    env.sleep(Duration.ofSeconds(5));
    List<Track> p = path(stub.query(Names.QUERY_STATE, LifecycleState.class));
    assertEquals(List.of(Track.TSA, Track.STP, Track.CLOSED, Track.TSA, Track.STP, Track.CLOSED), p);
  }

  @Test
  void reopenAfterCompletionContinuesStageVersionsInANewRun() {
    // Same Workflow ID, new run. Restarting at stage 1 would fail ClaimsPro fencing and reuse the
    // first run's idempotency keys (the reopened claim's payment would "replay" the old one).
    String id = "happy-" + UUID.randomUUID();
    LifecycleState first = start(id, new LifecycleSettings()).getResult(LifecycleState.class);
    assertEquals(3, first.getStageVersion());
    LifecycleState second = start(id, new LifecycleSettings(), true).getResult(LifecycleState.class);
    assertEquals(List.of(Track.TSA, Track.STP, Track.CLOSED), path(second));
    assertEquals(List.of(4, 5, 6), second.getTransitions().stream().map(t -> t.getStageVersion()).toList());
    assertEquals("claim reopened", second.getTransitions().get(0).getReason());
  }

  @Test
  void continueAsNewCarriesStateForward() {
    String id = "retriage-12-" + UUID.randomUUID(); // 5 STP visits re-triage, the 6th completes
    LifecycleState s = start(id, new LifecycleSettings().withMaxRetriage(10).withContinueAsNewEventThreshold(40))
        .getResult(LifecycleState.class);
    assertEquals(Track.CLOSED, s.getCurrentTrack());
    assertEquals(5, s.getRetriageCount());
    assertEquals(13, s.getStageVersion());
    // Every transition is still visible although the history was split across several runs.
    assertEquals(13, s.getTransitions().size());
  }
}
