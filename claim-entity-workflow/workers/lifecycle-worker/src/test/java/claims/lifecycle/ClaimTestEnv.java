package claims.lifecycle;

import claims.contracts.AppraisalResult;
import claims.contracts.ClaimRef;
import claims.contracts.ClaimSnapshot;
import claims.contracts.ClaimStart;
import claims.contracts.ContactResult;
import claims.contracts.CoverageAnalysis;
import claims.contracts.CoverageResult;
import claims.contracts.DispatchResult;
import claims.contracts.EstimateResult;
import claims.contracts.FlowDefinition;
import claims.contracts.FlowStage;
import claims.contracts.RiskAssessment;
import claims.contracts.StepRequest;
import claims.contracts.TriageInput;
import claims.contracts.TriageRuling;
import claims.lifecycle.workflows.ClaimWorkflow;
import claims.lifecycle.workflows.ClaimWorkflowImpl;
import claims.runtime.Names;
import claims.runtime.activities.CommHubActivities;
import claims.runtime.activities.EClaimsActivities;
import claims.runtime.activities.FlowActivities;
import claims.runtime.activities.InvestigationActivities;
import claims.runtime.activities.PolicyGateActivities;
import claims.runtime.activities.TriageActivities;
import claims.runtime.testing.FakeSystems;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import java.util.List;

/** Test server running ClaimWorkflow, with in-memory fakes for every track's activities. */
final class ClaimTestEnv implements AutoCloseable {
  final TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance();
  final FakeSystems systems = new FakeSystems();
  final FakeTracks tracks = new FakeTracks();

  ClaimTestEnv() {
    for (String name : new String[] {"ClaimId", "Stage", "Track", "OpenDecision"}) {
      env.registerSearchAttribute(name, IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    }
    var lifecycle = env.newWorker(Names.LIFECYCLE_QUEUE);
    lifecycle.registerWorkflowImplementationTypes(ClaimWorkflowImpl.class);
    lifecycle.registerActivitiesImplementations(systems);
    env.newWorker(Names.CLAIMSPRO_QUEUE).registerActivitiesImplementations(systems);
    env.newWorker(Names.TSA_QUEUE).registerActivitiesImplementations(tracks);
    env.newWorker(Names.IS_QUEUE).registerActivitiesImplementations(tracks);
    env.newWorker(Names.STP_QUEUE).registerActivitiesImplementations(tracks);
    env.start();
  }

  ClaimWorkflow start(String claimId, long amountCents) {
    systems.claim(claimId, amountCents);
    ClaimWorkflow wf = env.getWorkflowClient().newWorkflowStub(ClaimWorkflow.class,
        WorkflowOptions.newBuilder().setWorkflowId(Names.claimWorkflowId(claimId)).setTaskQueue(Names.LIFECYCLE_QUEUE).build());
    WorkflowClient.start(wf::run, ClaimStart.newBuilder().setClaimId(claimId).setStartEventId("created-" + claimId).build());
    return wf;
  }

  void awaitRow(String step) throws InterruptedException {
    for (int i = 0; i < 400 && systems.rows.stream().noneMatch(r -> r.getStep().equals(step)); i++) {
      Thread.sleep(25);
    }
  }

  @Override
  public void close() {
    env.close();
  }

  /** The Python-hosted triage and investigation activities and the Java STP activities, in memory. */
  static final class FakeTracks implements TriageActivities, InvestigationActivities, CommHubActivities,
      PolicyGateActivities, EClaimsActivities, FlowActivities {
    volatile boolean estimateAvailable = true;
    volatile long estimateCents = 400_000;

    @Override public RiskAssessment assessRisk(ClaimSnapshot c) { return RiskAssessment.newBuilder().setConfidence(0.95).build(); }
    @Override public TriageRuling applyRules(TriageInput in) {
      return TriageRuling.newBuilder().setRoute(in.getClaim().getInjuryReported() ? "MANUAL" : "STP").setReason("rules").build();
    }
    @Override public CoverageAnalysis analyzeCoverage(ClaimSnapshot c) { return CoverageAnalysis.newBuilder().setInformationComplete(true).build(); }
    @Override public ContactResult contactCustomer(StepRequest r) { return ContactResult.newBuilder().setReached(true).build(); }
    @Override public CoverageResult verifyCoverage(StepRequest r) { return CoverageResult.newBuilder().setCovered(true).build(); }
    @Override public DispatchResult dispatchVendor(StepRequest r) { return DispatchResult.newBuilder().setAccepted(true).setVendorId("V1").build(); }
    @Override public AppraisalResult assignAppraisal(StepRequest r) { return AppraisalResult.newBuilder().setAppraisalId("A1").build(); }
    @Override public EstimateResult getEstimate(ClaimRef ref) {
      return EstimateResult.newBuilder().setAvailable(estimateAvailable).setAmountCents(estimateCents).build();
    }
    @Override public FlowDefinition resolve(ClaimSnapshot c) {
      FlowDefinition.Builder f = FlowDefinition.newBuilder().setFlowId("test").setVersion(1).setContentHash("x")
          .setEstimateRecheckSeconds(60).setVendorsPerRound(3);
      for (List<String> stage : List.of(List.of("CONTACT_CUSTOMER"), List.of("VERIFY_COVERAGE", "OPEN_RESERVE"),
          List.of("DISPATCH", "ASSIGN_APPRAISAL"), List.of("AWAIT_ESTIMATE"), List.of("PAY"), List.of("CLOSE_CLAIM"))) {
        boolean money = stage.contains("PAY") || stage.contains("CLOSE_CLAIM");
        f.addStages(FlowStage.newBuilder().setName(stage.get(0).toLowerCase()).addAllSteps(stage).setInterruptible(!money));
      }
      return f.build();
    }
  }
}
