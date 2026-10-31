package claims.lifecycle;

import claims.contracts.v1.ClaimStart;
import claims.contracts.v1.TrackOutcome;
import claims.contracts.v1.TrackRequest;
import claims.contracts.v1.TrackResult;
import claims.lifecycle.workflows.ClaimLifecycleWorkflow;
import claims.lifecycle.workflows.ClaimLifecycleWorkflowImpl;
import claims.runtime.Names;
import claims.runtime.testing.FakeSystems;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Test server with the parent workflow, stub track children and in-memory fakes. */
final class LifecycleTestEnv implements AutoCloseable {
  /** Outcomes the stub tracks return, in order; HANG makes a track wait until cancelled. */
  static final Queue<TrackOutcome> TRIAGE = new ConcurrentLinkedQueue<>();
  static final Queue<TrackOutcome> STP = new ConcurrentLinkedQueue<>();
  static final TrackOutcome HANG = TrackOutcome.TRACK_OUTCOME_UNSPECIFIED;

  final TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance();
  final FakeSystems systems = new FakeSystems();

  LifecycleTestEnv() {
    TRIAGE.clear();
    STP.clear();
    for (String name : new String[] {"ClaimId", "Stage", "Track", "OpenDecision"}) {
      env.registerSearchAttribute(name, IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    }
    env.newWorker(Names.LIFECYCLE_QUEUE).registerWorkflowImplementationTypes(ClaimLifecycleWorkflowImpl.class);
    env.getWorkerFactory().getWorker(Names.LIFECYCLE_QUEUE).registerActivitiesImplementations(systems);
    env.newWorker(Names.CLAIMSPRO_QUEUE).registerActivitiesImplementations(systems);
    env.newWorker(Names.TSA_QUEUE).registerWorkflowImplementationTypes(TriageStubImpl.class);
    env.newWorker(Names.STP_QUEUE).registerWorkflowImplementationTypes(StpStubImpl.class);
    env.start();
  }

  ClaimLifecycleWorkflow start(String claimId) {
    systems.claim(claimId, 400_000);
    ClaimLifecycleWorkflow wf = env.getWorkflowClient().newWorkflowStub(ClaimLifecycleWorkflow.class,
        WorkflowOptions.newBuilder().setWorkflowId(Names.claimWorkflowId(claimId)).setTaskQueue(Names.LIFECYCLE_QUEUE).build());
    WorkflowClient.start(wf::run, ClaimStart.newBuilder().setClaimId(claimId).setStartEventId("created-1").build());
    return wf;
  }

  @Override
  public void close() {
    env.close();
  }

  static TrackResult next(Queue<TrackOutcome> q) {
    TrackOutcome o = q.poll();
    if (o == null || o == HANG) {
      Workflow.await(() -> false); // until cancelled by the parent
    }
    return TrackResult.newBuilder().setOutcome(o).setReason("stub").build();
  }

  @WorkflowInterface
  public interface TriageStub {
    @WorkflowMethod(name = Names.TRIAGE)
    TrackResult run(TrackRequest r);
  }

  public static class TriageStubImpl implements TriageStub {
    @Override public TrackResult run(TrackRequest r) { return next(TRIAGE); }
  }

  @WorkflowInterface
  public interface StpStub {
    @WorkflowMethod(name = Names.STP)
    TrackResult run(TrackRequest r);
  }

  public static class StpStubImpl implements StpStub {
    @Override public TrackResult run(TrackRequest r) { return next(STP); }
  }
}
