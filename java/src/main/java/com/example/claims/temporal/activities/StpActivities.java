package com.example.claims.temporal.activities;

import com.example.claims.temporal.workflows.stp.flow.FlowDefinition;
import io.temporal.activity.ActivityInterface;
import java.util.List;

/**
 * Every STP side effect. Each method takes a reference (StepContext), loads what it needs from
 * ClaimsPro, and is safe to run more than once: writes carry the step's idempotency key and the
 * stage version lease.
 */
@ActivityInterface
public interface StpActivities {
  ClaimSnapshot fetchClaim(String claimId);

  FlowDefinition resolveFlow(ClaimSnapshot claim);

  StepResult initialContact(StepContext ctx);

  StepResult verifyCoverage(StepContext ctx);

  StepResult openReserve(StepContext ctx);

  StepResult assignAppraisal(StepContext ctx);

  StepResult requestEstimate(StepContext ctx);

  /** Backstop poll: is the estimate there even if the ESTIMATE_RECEIVED event was missed? */
  StepResult checkEstimate(StepContext ctx);

  StepResult issuePayment(StepContext ctx, long amountCents);

  StepResult finalizePayment(StepContext ctx, String paymentId);

  StepResult cancelPayment(StepContext ctx, String paymentId);

  StepResult closeClaim(StepContext ctx);

  List<String> listVendors(StepContext ctx);

  StepResult offerDispatch(StepContext ctx, String vendorId);
}
