package claims.runtime.testing;

import claims.contracts.ClaimRef;
import claims.contracts.ClaimSnapshot;
import claims.contracts.CloseClaimRequest;
import claims.contracts.CloseTaskRequest;
import claims.contracts.CreatePaymentRequest;
import claims.contracts.DecisionPolicy;
import claims.contracts.DecisionRule;
import claims.contracts.Done;
import claims.contracts.OpenDecision;
import claims.contracts.PaymentKeyRef;
import claims.contracts.PaymentRecord;
import claims.contracts.PolicyRequest;
import claims.contracts.ProjectionEvent;
import claims.contracts.RecordCheck;
import claims.contracts.ReserveRequest;
import claims.contracts.ReserveResult;
import claims.contracts.StageChange;
import claims.contracts.StageInfo;
import claims.contracts.TaskRef;
import claims.contracts.TaskRequest;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.activities.PolicyActivities;
import claims.runtime.activities.ProjectionActivities;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory ClaimsPro, projection table and policy for workflow tests. */
public class FakeSystems implements ClaimsProActivities, ProjectionActivities, PolicyActivities {
  public static final Done DONE = Done.newBuilder().setOk(true).build();

  public final Map<String, ClaimSnapshot> claims = new ConcurrentHashMap<>();
  public final Map<String, OpenDecision> openTasks = new ConcurrentHashMap<>();
  public final Map<String, PaymentRecord> payments = new ConcurrentHashMap<>();
  public final AtomicInteger paymentCreates = new AtomicInteger();
  private final Map<String, CreatePaymentRequest> paymentRequests = new ConcurrentHashMap<>();
  public final List<ProjectionEvent> rows = new CopyOnWriteArrayList<>();
  public final Map<String, StageChange> stages = new ConcurrentHashMap<>();
  public final List<String> closedClaims = new CopyOnWriteArrayList<>();
  public volatile DecisionPolicy policy = defaultPolicy();
  private final AtomicInteger ids = new AtomicInteger(1);

  public static DecisionPolicy defaultPolicy() {
    DecisionRule rule = DecisionRule.newBuilder().setRole("adjuster").setDeadlineSeconds(86_400).build();
    return DecisionPolicy.newBuilder()
        .setRetryWindowSeconds(60).setRetriageLimit(3).setStpPaymentAuthorityCents(1_500_000)
        .setTriageConfidenceThreshold(0.6)
        .putRules("MANUAL_HANDLING", rule)
        .putRules("PAYMENT_APPROVAL", rule.toBuilder().setRole("payment-approver").addReasonRequiredChoices("REJECT").build())
        .putRules("ESTIMATE_REVIEW", rule.toBuilder().setRole("appraiser").build())
        .putRules("OPERATIONS_EXCEPTION", DecisionRule.newBuilder().setRole("claims-operations").build())
        .build();
  }

  public FakeSystems claim(String claimId, long amountCents) {
    claims.put(claimId, ClaimSnapshot.newBuilder().setClaimId(claimId).setStatus("OPEN").setLineOfBusiness("AUTO")
        .setCompany("CO1").setChannel("WEB").setState("OH").setChangeVersion(1).setClaimedAmountCents(amountCents)
        .setAdjusterId("adj-1").build());
    return this;
  }

  /** The open task of a kind, waiting until the workflow has created it. */
  public OpenDecision awaitTask(String kind) throws InterruptedException {
    for (int i = 0; i < 400; i++) {
      for (OpenDecision d : openTasks.values()) {
        if (d.getKind().name().equals(kind)) {
          return d;
        }
      }
      Thread.sleep(25);
    }
    throw new AssertionError("No open task " + kind);
  }

  @Override public ClaimSnapshot readClaim(ClaimRef ref) {
    long paid = paymentRequests.entrySet().stream()
        .filter(e -> e.getValue().getStep().getClaimId().equals(ref.getClaimId()))
        .filter(e -> payments.get(e.getKey()) == null || !"CANCELLED".equals(payments.get(e.getKey()).getStatus()))
        .mapToLong(e -> e.getValue().getAmountCents()).sum();
    return claims.get(ref.getClaimId()).toBuilder().setPaidCents(paid).build();
  }

  @Override public TaskRef createTask(TaskRequest r) {
    String id = "TASK-" + ids.getAndIncrement();
    openTasks.put(id, r.getDecision().toBuilder().setTaskId(id).build());
    return TaskRef.newBuilder().setTaskId(id).build();
  }

  @Override public Done closeTask(CloseTaskRequest r) { openTasks.remove(r.getTaskId()); return DONE; }

  @Override public ReserveResult openReserve(ReserveRequest r) { return ReserveResult.newBuilder().setReserveId("RES-1").build(); }

  @Override public PaymentRecord createPayment(CreatePaymentRequest r) {
    return payments.computeIfAbsent(r.getPaymentKey(), k -> {
      paymentCreates.incrementAndGet();
      paymentRequests.put(k, r);
      return PaymentRecord.newBuilder().setPaymentId("PAY-" + ids.getAndIncrement()).setStatus("CREATED").build();
    });
  }

  @Override public PaymentRecord closePayment(PaymentKeyRef r) {
    return payments.computeIfPresent(r.getPaymentKey(), (k, p) -> p.toBuilder().setStatus("CLOSED").build());
  }

  @Override public PaymentRecord cancelPayment(PaymentKeyRef r) {
    return payments.computeIfPresent(r.getPaymentKey(), (k, p) -> p.toBuilder().setStatus("CANCELLED").build());
  }

  @Override public Done closeClaim(CloseClaimRequest r) { closedClaims.add(r.getStep().getClaimId()); return DONE; }

  @Override public Done record(ProjectionEvent e) { rows.add(e); return DONE; }

  @Override public Done beginStage(StageChange c) { stages.put(c.getStep().getClaimId(), c); return DONE; }

  @Override public StageInfo currentStage(ClaimRef ref) {
    StageChange c = stages.get(ref.getClaimId());
    return c == null ? StageInfo.getDefaultInstance()
        : StageInfo.newBuilder().setStageVersion(c.getStep().getStageVersion()).setStage(c.getStage()).build();
  }

  @Override public Done verifyRecord(RecordCheck check) { return DONE; }

  @Override public DecisionPolicy load(PolicyRequest request) { return policy; }
}
