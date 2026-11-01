package claims.stp.mock;

import claims.contracts.ClaimRef;
import claims.contracts.ClaimSnapshot;
import claims.contracts.CloseClaimRequest;
import claims.contracts.CloseTaskRequest;
import claims.contracts.CreatePaymentRequest;
import claims.contracts.Done;
import claims.contracts.OpenDecision;
import claims.contracts.PaymentKeyRef;
import claims.contracts.PaymentRecord;
import claims.contracts.ReserveRequest;
import claims.contracts.ReserveResult;
import claims.contracts.TaskRef;
import claims.contracts.TaskRequest;
import claims.runtime.activities.ActivityErrors;
import claims.runtime.activities.ClaimsProActivities;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Mock ClaimsPro over the mock schema. Replace with the real ClaimsPro service calls. */
public class MockClaimsProActivities implements ClaimsProActivities {
  private static final Done DONE = Done.newBuilder().setOk(true).build();
  private final MockDataStore mock;

  public MockClaimsProActivities(MockDataStore mock) {
    this.mock = mock;
  }

  @Override
  public ClaimSnapshot readClaim(ClaimRef ref) {
    mock.maybeFail("ClaimsPro");
    String sql = """
        SELECT claim_id, status, line_of_business, company, channel, state, change_version, injury_reported,
               prior_claims, claimed_amount_cents, adjuster_id,
               (SELECT coalesce(sum(p.amount_cents), 0) FROM mock.payments p
                 WHERE p.claim_id = c.claim_id AND p.status IN ('CREATED', 'CLOSED'))
        FROM mock.claims c WHERE c.claim_id = ?""";
    try (Connection c = mock.connection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, ref.getClaimId());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw ActivityErrors.notFound("Claim " + ref.getClaimId() + " not in ClaimsPro");
        }
        return ClaimSnapshot.newBuilder()
            .setClaimId(rs.getString(1)).setStatus(rs.getString(2)).setLineOfBusiness(rs.getString(3))
            .setCompany(rs.getString(4)).setChannel(rs.getString(5)).setState(rs.getString(6))
            .setChangeVersion(rs.getLong(7)).setInjuryReported(rs.getBoolean(8)).setPriorClaims(rs.getInt(9))
            .setClaimedAmountCents(rs.getLong(10)).setAdjusterId(rs.getString(11)).setPaidCents(rs.getLong(12)).build();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public TaskRef createTask(TaskRequest request) {
    mock.maybeFail("ClaimsPro");
    OpenDecision d = request.getDecision();
    String sql = """
        INSERT INTO mock.tasks (decision_id, workflow_id, claim_id, kind, choices, required_role,
          required_authority_cents, requested_by, level, context, due_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (decision_id) DO UPDATE SET decision_id = excluded.decision_id
        RETURNING task_id""";
    try (Connection c = mock.connection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, d.getDecisionId());
      ps.setString(2, d.getWorkflowId());
      ps.setString(3, d.getClaimId());
      ps.setString(4, d.getKind().name());
      ps.setArray(5, c.createArrayOf("text", d.getChoicesList().toArray()));
      ps.setString(6, d.getRequiredRole());
      ps.setLong(7, d.getRequiredAuthorityCents());
      ps.setString(8, d.getRequestedBy());
      ps.setInt(9, d.getLevel());
      ps.setString(10, d.getContext());
      ps.setLong(11, d.getDueAtMs());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return TaskRef.newBuilder().setTaskId(rs.getString(1)).build();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Done closeTask(CloseTaskRequest request) {
    mock.maybeFail("ClaimsPro");
    update("UPDATE mock.tasks SET status = 'CLOSED', outcome = ?, closed_at = now() WHERE task_id = ? AND status = 'OPEN'",
        request.getOutcome(), request.getTaskId());
    return DONE;
  }

  @Override
  public ReserveResult openReserve(ReserveRequest request) {
    mock.maybeFail("ClaimsPro");
    String sql = """
        INSERT INTO mock.reserves (idempotency_key, claim_id, amount_cents) VALUES (?, ?, ?)
        ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = excluded.idempotency_key
        RETURNING reserve_id""";
    try (Connection c = mock.connection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, MockDataStore.key(request.getStep()));
      ps.setString(2, request.getStep().getClaimId());
      ps.setLong(3, request.getAmountCents());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return ReserveResult.newBuilder().setReserveId(rs.getString(1)).build();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Looks up the payment key first: a rerun returns the existing payment instead of paying twice. */
  @Override
  public PaymentRecord createPayment(CreatePaymentRequest request) {
    mock.maybeFail("ClaimsPro");
    PaymentRecord existing = find(request.getPaymentKey());
    if (existing != null) {
      return existing;
    }
    update("INSERT INTO mock.payments (payment_key, claim_id, amount_cents, payee_ref) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
        request.getPaymentKey(), request.getStep().getClaimId(), request.getAmountCents(), request.getPayeeRef());
    if (mock.behaviour(request.getStep().getClaimId(), "claimsPro").path("payment_ack_lost_once").asBoolean()
        && update("UPDATE mock.payments SET ack_lost = true WHERE payment_key = ? AND NOT ack_lost", request.getPaymentKey()) > 0) {
      // The payment was created but the reply is lost: Temporal retries, and the lookup above finds it.
      throw new IllegalStateException("ClaimsPro connection reset after the payment was created (simulated)");
    }
    return find(request.getPaymentKey());
  }

  @Override
  public PaymentRecord closePayment(PaymentKeyRef request) {
    mock.maybeFail("ClaimsPro");
    update("UPDATE mock.payments SET status = 'CLOSED', closed_at = now() WHERE payment_key = ? AND status = 'CREATED'",
        request.getPaymentKey());
    return require(request.getPaymentKey());
  }

  @Override
  public PaymentRecord cancelPayment(PaymentKeyRef request) {
    mock.maybeFail("ClaimsPro");
    PaymentRecord p = require(request.getPaymentKey());
    if ("CLOSED".equals(p.getStatus())) {
      throw ActivityErrors.refused("Payment " + request.getPaymentKey() + " is closed");
    }
    update("UPDATE mock.payments SET status = 'CANCELLED' WHERE payment_key = ?", request.getPaymentKey());
    return require(request.getPaymentKey());
  }

  @Override
  public Done closeClaim(CloseClaimRequest request) {
    mock.maybeFail("ClaimsPro");
    update("UPDATE mock.claims SET status = 'CLOSED', closed_reason = ?, updated_at = now() WHERE claim_id = ?",
        request.getReason(), request.getStep().getClaimId());
    return DONE;
  }

  private PaymentRecord require(String key) {
    PaymentRecord p = find(key);
    if (p == null) {
      throw ActivityErrors.notFound("Payment " + key + " not found");
    }
    return p;
  }

  private PaymentRecord find(String key) {
    try (Connection c = mock.connection();
        PreparedStatement ps = c.prepareStatement("SELECT payment_id, status FROM mock.payments WHERE payment_key = ?")) {
      ps.setString(1, key);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? PaymentRecord.newBuilder().setPaymentId(rs.getString(1)).setStatus(rs.getString(2)).build() : null;
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private int update(String sql, Object... args) {
    try (Connection c = mock.connection(); PreparedStatement ps = c.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        ps.setObject(i + 1, args[i]);
      }
      return ps.executeUpdate();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
