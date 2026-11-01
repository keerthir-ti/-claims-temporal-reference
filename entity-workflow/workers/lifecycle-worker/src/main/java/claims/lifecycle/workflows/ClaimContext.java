package claims.lifecycle.workflows;

import claims.contracts.ClaimRef;
import claims.contracts.DecisionPolicy;
import claims.contracts.StepRef;
import claims.contracts.StepRequest;
import claims.runtime.Names;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.activities.CommHubActivities;
import claims.runtime.activities.EClaimsActivities;
import claims.runtime.activities.FlowActivities;
import claims.runtime.activities.InvestigationActivities;
import claims.runtime.activities.PolicyGateActivities;
import claims.runtime.activities.TriageActivities;
import claims.runtime.workflow.ActivityStubs;
import claims.runtime.workflow.DecisionDesk;
import claims.runtime.workflow.HistoryRecorder;
import claims.runtime.workflow.StepRunner;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

/**
 * What the claim workflow's modules share: the claim, its current stage version, the business-history
 * recorder, the decision desk, the step runner, the activity stubs for every track's task queue, and the
 * flags set by incoming events. Workflow code only.
 */
final class ClaimContext {
  final String claimId;
  private final IntSupplier stageVersion;
  final HistoryRecorder recorder;
  final DecisionDesk desk;
  final StepRunner steps;
  DecisionPolicy policy = DecisionPolicy.getDefaultInstance();

  ClaimsProActivities claimsPro;
  ClaimsProActivities claimsProUntilDone;
  TriageActivities triage;
  InvestigationActivities investigation;
  CommHubActivities commHub;
  PolicyGateActivities policyGate;
  EClaimsActivities eClaims;
  FlowActivities flows;

  // Set by signal handlers; each module clears what it reads.
  boolean claimChanged;
  boolean estimateReceived;
  boolean documentsReceived;
  boolean cancelPaymentRequested;
  String cancelPaymentBy = "";

  /** Payments run outside every cancellation scope; the workflow waits for them before it completes. */
  final List<Promise<Void>> payments = new ArrayList<>();

  ClaimContext(String claimId, IntSupplier stageVersion, HistoryRecorder recorder, DecisionDesk desk) {
    this.claimId = claimId;
    this.stageVersion = stageVersion;
    this.recorder = recorder;
    this.desk = desk;
    this.steps = new StepRunner(desk);
  }

  void usePolicy(DecisionPolicy policy) {
    this.policy = policy;
    desk.setPolicy(policy);
    Duration window = ActivityStubs.retryWindow(policy);
    claimsPro = ActivityStubs.claimsPro(window);
    claimsProUntilDone = ActivityStubs.claimsPro(null);
    commHub = ActivityStubs.on(CommHubActivities.class, Names.STP_QUEUE, Duration.ofSeconds(30), window);
    policyGate = ActivityStubs.on(PolicyGateActivities.class, Names.STP_QUEUE, Duration.ofSeconds(30), window);
    eClaims = ActivityStubs.on(EClaimsActivities.class, Names.STP_QUEUE, Duration.ofSeconds(30), window);
    flows = ActivityStubs.on(FlowActivities.class, Names.STP_QUEUE, Duration.ofSeconds(10), window);
    // AI calls are capped: an AI outage routes the claim to a person instead of retrying a paid model forever.
    triage = Workflow.newActivityStub(TriageActivities.class, aiOptions(Names.TSA_QUEUE, window));
    investigation = Workflow.newActivityStub(InvestigationActivities.class, aiOptions(Names.IS_QUEUE, window));
  }

  private static ActivityOptions aiOptions(String queue, Duration window) {
    ActivityOptions o = ActivityStubs.options(queue, Duration.ofSeconds(60), window);
    return ActivityOptions.newBuilder(o)
        .setRetryOptions(RetryOptions.newBuilder(o.getRetryOptions()).setMaximumAttempts(5).build()).build();
  }

  int stage() {
    return stageVersion.getAsInt();
  }

  ClaimRef ref() {
    return ClaimRef.newBuilder().setClaimId(claimId).build();
  }

  StepRef step(String name) {
    return StepRef.newBuilder().setClaimId(claimId).setStageVersion(stage()).setStep(name).build();
  }

  StepRequest stepRequest(String name) {
    return StepRequest.newBuilder().setStep(step(name)).build();
  }
}
