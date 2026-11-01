package claims.runtime.workflow;

import claims.contracts.CloseTaskRequest;
import claims.contracts.Decision;
import claims.contracts.DecisionKind;
import claims.contracts.DecisionPolicy;
import claims.contracts.DecisionResult;
import claims.contracts.DecisionRule;
import claims.contracts.OpenDecision;
import claims.contracts.StepRef;
import claims.contracts.TaskRequest;
import claims.runtime.SearchAttrs;
import claims.runtime.activities.ClaimsProActivities;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Human decisions inside a workflow. The workflow creates an adjuster task in ClaimsPro carrying its
 * workflow ID and a decision ID, then waits with a deadline. The adjuster's answer arrives as the
 * {@code decide} Update: {@link #validate} rejects it before it is recorded, {@link #handle} answers at
 * once. At the deadline the task moves to the next escalation role; after the last one the caller gets
 * {@link #DEADLINE_PASSED}.
 */
public final class DecisionDesk {
  public static final String DEADLINE_PASSED = "DEADLINE_PASSED";

  private final String claimId;
  private final IntSupplier stageVersion;
  private final HistoryRecorder recorder;
  private final ClaimsProActivities claimsPro;
  private final Map<String, OpenDecision> open = new LinkedHashMap<>();
  private final Map<String, DecisionResult> decided = new HashMap<>();
  private DecisionPolicy policy = DecisionPolicy.getDefaultInstance();
  private int counter;

  public DecisionDesk(String claimId, IntSupplier stageVersion, HistoryRecorder recorder) {
    this.claimId = claimId;
    this.stageVersion = stageVersion;
    this.recorder = recorder;
    // Tasks are how people reach the claim, including the operations task for an outage: retry until ClaimsPro answers.
    this.claimsPro = ActivityStubs.claimsPro(null);
  }

  public void setPolicy(DecisionPolicy policy) {
    this.policy = policy;
  }

  /**
   * Opens a decision and waits for it. Returns null when {@code interrupt} becomes true first (the
   * task is then withdrawn).
   */
  public DecisionResult decide(DecisionKind kind, List<String> choices, String context,
      long requiredAuthorityCents, String requestedBy, Supplier<Boolean> interrupt) {
    DecisionRule rule = policy.getRulesOrDefault(kind.name(), DecisionRule.getDefaultInstance());
    List<String> roles = new ArrayList<>();
    roles.add(rule.getRole());
    roles.addAll(rule.getEscalationRolesList());
    String workflowId = Workflow.getInfo().getWorkflowId();
    int stage = stageVersion.getAsInt();

    for (int level = 0; level < roles.size(); level++) {
      String id = workflowId + ":v" + stage + ":" + kind.name().toLowerCase() + ":" + (++counter);
      long deadline = rule.getDeadlineSeconds();
      OpenDecision.Builder od = OpenDecision.newBuilder()
          .setDecisionId(id).setKind(kind).setClaimId(claimId).setWorkflowId(workflowId)
          .addAllChoices(choices).setRequiredRole(roles.get(level))
          .setRequiredAuthorityCents(requiredAuthorityCents).setRequestedBy(requestedBy == null ? "" : requestedBy)
          .setLevel(level).setContext(context == null ? "" : context)
          .addAllReasonRequiredChoices(rule.getReasonRequiredChoicesList())
          .setDueAtMs(deadline > 0 ? Workflow.currentTimeMillis() + deadline * 1000 : 0);
      // Open before the task exists: an adjuster may answer as soon as the task appears.
      open.put(id, od.build());
      String taskId = claimsPro.createTask(TaskRequest.newBuilder()
          .setStep(step(stage, "task:" + id)).setDecision(od).build()).getTaskId();
      if (open.containsKey(id)) {
        open.put(id, od.setTaskId(taskId).build());
      }
      Workflow.upsertTypedSearchAttributes(SearchAttrs.OPEN_DECISION.valueSet(kind.name()));
      recorder.row(stage, "decision.open:" + id).outcome(roles.get(level)).ref("task_id", taskId)
          .reason(context).write();

      Supplier<Boolean> ready = () -> decided.containsKey(id) || interrupt.get();
      if (deadline > 0) {
        Workflow.await(Duration.ofSeconds(deadline), ready);
      } else {
        Workflow.await(ready);
      }
      open.remove(id);
      if (open.isEmpty()) {
        Workflow.upsertTypedSearchAttributes(SearchAttrs.OPEN_DECISION.valueUnset());
      }

      DecisionResult result = decided.get(id);
      if (result != null) {
        closeTask(stage, taskId, result.getChoice());
        recorder.row(stage, "decision:" + id).outcome(result.getChoice()).actor(result.getDecidedBy())
            .reason(result.getReason()).trigger(id).ref("task_id", taskId).write();
        return result;
      }
      if (interrupt.get()) {
        closeTask(stage, taskId, "WITHDRAWN");
        recorder.row(stage, "decision.withdrawn:" + id).outcome("WITHDRAWN").ref("task_id", taskId).write();
        return null;
      }
      closeTask(stage, taskId, "ESCALATED");
      recorder.row(stage, "decision.deadline:" + id).outcome("DEADLINE_PASSED").ref("task_id", taskId).write();
    }
    return DecisionResult.newBuilder().setChoice(DEADLINE_PASSED).build();
  }

  /** Update validator: throwing rejects the decision before anything is recorded. */
  public void validate(Decision d) {
    if (decided.containsKey(d.getDecisionId())) {
      return; // a repeated decision ID returns the first result
    }
    OpenDecision od = open.get(d.getDecisionId());
    if (od == null) {
      throw new IllegalArgumentException("No open decision " + d.getDecisionId());
    }
    if (!od.getChoicesList().contains(d.getChoice())) {
      throw new IllegalArgumentException("Choice " + d.getChoice() + " is not one of " + od.getChoicesList());
    }
    if (!od.getRequiredRole().isEmpty() && !d.getActorRolesList().contains(od.getRequiredRole())) {
      throw new IllegalArgumentException("Role " + od.getRequiredRole() + " required");
    }
    if (od.getRequiredAuthorityCents() > 0 && d.getActorAuthorityCents() < od.getRequiredAuthorityCents()) {
      throw new IllegalArgumentException("Authority of " + od.getRequiredAuthorityCents() + " cents required");
    }
    if (!od.getRequestedBy().isEmpty() && od.getRequestedBy().equals(d.getActorId())) {
      throw new IllegalArgumentException("The requester cannot approve their own request");
    }
    if (od.getReasonRequiredChoicesList().contains(d.getChoice()) && d.getReason().isBlank()) {
      throw new IllegalArgumentException("A reason is required for " + d.getChoice());
    }
  }

  /** Update handler: records the decision and answers the caller at once. */
  public DecisionResult handle(Decision d) {
    return decided.computeIfAbsent(d.getDecisionId(), id -> DecisionResult.newBuilder()
        .setDecisionId(id).setChoice(d.getChoice()).setDecidedBy(d.getActorId()).setReason(d.getReason()).build());
  }

  public boolean hasOpen() {
    return !open.isEmpty();
  }

  /** Withdraws every open task; used when the workflow is cancelled (call from a detached scope). */
  public void withdrawAll() {
    int stage = stageVersion.getAsInt();
    for (OpenDecision od : new ArrayList<>(open.values())) {
      closeTask(stage, od.getTaskId(), "WITHDRAWN");
    }
    open.clear();
  }

  private void closeTask(int stage, String taskId, String outcome) {
    claimsPro.closeTask(CloseTaskRequest.newBuilder()
        .setStep(step(stage, "task.close:" + taskId)).setTaskId(taskId).setOutcome(outcome).build());
  }

  private StepRef step(int stage, String name) {
    return StepRef.newBuilder().setClaimId(claimId).setStageVersion(stage).setStep(name).build();
  }
}
