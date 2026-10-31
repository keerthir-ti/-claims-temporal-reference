package claims.stp;

import claims.contracts.v1.AppraisalResult;
import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.ClaimSnapshot;
import claims.contracts.v1.ContactResult;
import claims.contracts.v1.CoverageResult;
import claims.contracts.v1.DispatchResult;
import claims.contracts.v1.EstimateResult;
import claims.contracts.v1.FlowDefinition;
import claims.contracts.v1.StepRequest;
import claims.runtime.Names;
import claims.runtime.testing.FakeSystems;
import claims.stp.activities.CommHubActivities;
import claims.stp.activities.EClaimsActivities;
import claims.stp.activities.FlowActivities;
import claims.stp.activities.PolicyGateActivities;
import claims.stp.workflows.PaymentWorkflowImpl;
import claims.stp.workflows.StpWorkflowImpl;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;

/** Test server with the STP worker's workflows and in-memory fakes for every activity. */
final class StpTestEnv implements AutoCloseable {
  final TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance();
  final FakeSystems systems = new FakeSystems();
  final FakeStp stp = new FakeStp();

  StpTestEnv() {
    for (String name : new String[] {"ClaimId", "Stage", "Track", "OpenDecision"}) {
      env.registerSearchAttribute(name, IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    }
    Worker w = env.newWorker(Names.STP_QUEUE);
    w.registerWorkflowImplementationTypes(StpWorkflowImpl.class, PaymentWorkflowImpl.class);
    w.registerActivitiesImplementations(stp);
    env.newWorker(Names.LIFECYCLE_QUEUE).registerActivitiesImplementations(systems);
    env.newWorker(Names.CLAIMSPRO_QUEUE).registerActivitiesImplementations(systems);
    env.start();
  }

  @Override
  public void close() {
    env.close();
  }

  static final class FakeStp implements CommHubActivities, PolicyGateActivities, EClaimsActivities, FlowActivities {
    volatile boolean estimateAvailable = true;
    volatile long estimateCents = 400_000;

    @Override public ContactResult contactCustomer(StepRequest r) { return ContactResult.newBuilder().setReached(true).build(); }
    @Override public CoverageResult verifyCoverage(StepRequest r) { return CoverageResult.newBuilder().setCovered(true).build(); }
    @Override public DispatchResult dispatchVendor(StepRequest r) { return DispatchResult.newBuilder().setAccepted(true).setVendorId("V1").build(); }
    @Override public AppraisalResult assignAppraisal(StepRequest r) { return AppraisalResult.newBuilder().setAppraisalId("A1").build(); }
    @Override public EstimateResult getEstimate(ClaimRef ref) {
      return EstimateResult.newBuilder().setAvailable(estimateAvailable).setAmountCents(estimateCents).build();
    }
    @Override public FlowDefinition resolve(ClaimSnapshot c) {
      return FlowDefinition.newBuilder().setFlowId("test").setVersion(1).setContentHash("x")
          .addSteps("CONTACT_CUSTOMER").addSteps("VERIFY_COVERAGE").addSteps("OPEN_RESERVE")
          .addSteps("DISPATCH_AND_APPRAISAL").addSteps("AWAIT_ESTIMATE").addSteps("PAY").addSteps("CLOSE_CLAIM")
          .setEstimateRecheckSeconds(60).build();
    }
  }
}
