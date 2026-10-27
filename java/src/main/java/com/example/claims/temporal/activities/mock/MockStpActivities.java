package com.example.claims.temporal.activities.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.example.claims.temporal.activities.ClaimSnapshot;
import com.example.claims.temporal.activities.StepContext;
import com.example.claims.temporal.activities.StepResult;
import com.example.claims.temporal.activities.StpActivities;
import com.example.claims.temporal.client.ContractJson;
import com.example.claims.temporal.workflows.stp.flow.FlowDefinition;
import com.example.claims.temporal.workflows.stp.flow.FlowRegistry;
import io.temporal.activity.Activity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MOCK of the STP activities. Each method stands in for one call to an external system (system named
 * per method); data comes from the mock-data service. Replace this class with the real
 * implementation of {@link StpActivities}; the workflows do not change.
 *
 * <p>Exception: {@link #resolveFlow} is real (it reads config/flows) and should be kept as is.
 */
public class MockStpActivities implements StpActivities {
  private static final Logger log = LoggerFactory.getLogger(MockStpActivities.class);

  private final MockSystemsClient systems;
  private final Supplier<FlowRegistry> flows;

  /** @param flows supplier so flow files can be reloaded without restarting the worker */
  public MockStpActivities(MockSystemsClient systems, Supplier<FlowRegistry> flows) {
    this.systems = systems;
    this.flows = flows;
  }

  /** ClaimsPro: current claim data. */
  @Override
  public ClaimSnapshot fetchClaim(String claimId) {
    return ContractJson.MAPPER.convertValue(systems.get("/claimspro/claims/" + claimId), ClaimSnapshot.class);
  }

  /** REAL: selects and pins the YAML flow for this claim. */
  @Override
  public FlowDefinition resolveFlow(ClaimSnapshot claim) {
    FlowDefinition flow = flows.get().select(claim);
    log.info("Resolved STP flow {} hash={} for claim {}", flow.ref(), flow.contentHash(), claim.claimId());
    return flow;
  }

  /** Comm Hub: contact the customer. */
  @Override
  public StepResult initialContact(StepContext ctx) {
    boolean reachable = systems.post("/commhub/contacts", Map.of("claimId", ctx.claimId()), ctx).path("reachable").asBoolean();
    return reachable ? StepResult.ok("customer contacted") : StepResult.handoff("customer unreachable");
  }

  /** Policy Gate: coverage decision. STP cannot route to Investigation itself; it asks for re-triage. */
  @Override
  public StepResult verifyCoverage(StepContext ctx) {
    String status = systems.get("/policygate/coverage/" + ctx.claimId()).path("coverageStatus").asText();
    return "CONFIRMED".equals(status) ? StepResult.ok("coverage confirmed") : StepResult.retriage("coverage " + status);
  }

  /** ClaimsPro: open the reserve. */
  @Override
  public StepResult openReserve(StepContext ctx) {
    JsonNode r = systems.post("/claimspro/claims/" + ctx.claimId() + "/reserves", Map.of(), ctx);
    return StepResult.ok("reserve opened", r.path("reserveId").asText(), r.path("amountCents").asLong());
  }

  /** eClaims: assign an appraiser. */
  @Override
  public StepResult assignAppraisal(StepContext ctx) {
    String appraiserId = systems.post("/eclaims/claims/" + ctx.claimId() + "/appraisals", Map.of(), ctx)
        .path("appraiserId").asText();
    return StepResult.ok("appraiser " + appraiserId, appraiserId, null);
  }

  /** eClaims: ask the vendor for an estimate (it arrives later as an event). */
  @Override
  public StepResult requestEstimate(StepContext ctx) {
    systems.post("/eclaims/claims/" + ctx.claimId() + "/estimates", Map.of(), ctx);
    return StepResult.ok("estimate requested");
  }

  /** eClaims: is the estimate ready? Used on the event and by the backstop poll. */
  @Override
  public StepResult checkEstimate(StepContext ctx) {
    JsonNode r = systems.get("/eclaims/claims/" + ctx.claimId() + "/estimate");
    return r.path("ready").asBoolean()
        ? StepResult.ok("estimate ready", null, r.path("amountCents").asLong())
        : new StepResult("PENDING", "estimate not ready", null, null);
  }

  /** ClaimsPro: create the payment. Same Idempotency-Key on every retry, so a retry never pays twice. */
  @Override
  public StepResult issuePayment(StepContext ctx, long amountCents) {
    JsonNode r = systems.post("/claimspro/claims/" + ctx.claimId() + "/payments", Map.of("amountCents", amountCents), ctx);
    log.info("Payment {} amountCents={} replayed={} attempt={}", r.path("paymentId").asText(), amountCents,
        r.path("replayed").asBoolean(), Activity.getExecutionContext().getInfo().getAttempt());
    return StepResult.ok("payment issued", r.path("paymentId").asText(), amountCents);
  }

  /** ClaimsPro: close the payment. */
  @Override
  public StepResult finalizePayment(StepContext ctx, String paymentId) {
    systems.post("/claimspro/claims/" + ctx.claimId() + "/payments/" + paymentId + "/finalize", Map.of(), ctx);
    return StepResult.ok("payment finalized", paymentId, null);
  }

  /** ClaimsPro: cancel the payment. */
  @Override
  public StepResult cancelPayment(StepContext ctx, String paymentId) {
    systems.post("/claimspro/claims/" + ctx.claimId() + "/payments/" + paymentId + "/cancel", Map.of(), ctx);
    return StepResult.ok("payment cancelled", paymentId, null);
  }

  /** ClaimsPro: close the claim. */
  @Override
  public StepResult closeClaim(StepContext ctx) {
    systems.post("/claimspro/claims/" + ctx.claimId() + "/close", Map.of(), ctx);
    return StepResult.ok("claim closed");
  }

  /** eClaims: candidate vendors, in preference order. */
  @Override
  public List<String> listVendors(StepContext ctx) {
    List<String> ids = new ArrayList<>();
    systems.get("/eclaims/claims/" + ctx.claimId() + "/vendors").path("vendors")
        .forEach(v -> ids.add(v.path("vendorId").asText()));
    return ids;
  }

  /** eClaims: offer the dispatch to one vendor. */
  @Override
  public StepResult offerDispatch(StepContext ctx, String vendorId) {
    StepContext perVendor = new StepContext(ctx.claimId(), ctx.stageVersion(), ctx.step() + ":" + vendorId);
    boolean accepted = systems.post("/eclaims/claims/" + ctx.claimId() + "/dispatch", Map.of("vendorId", vendorId), perVendor)
        .path("accepted").asBoolean();
    return accepted ? StepResult.ok("vendor accepted", vendorId, null) : new StepResult("DECLINED", "vendor declined", vendorId, null);
  }
}
