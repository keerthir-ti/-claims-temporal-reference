package claims.stp.mock;

import claims.contracts.v1.AppraisalResult;
import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.ContactResult;
import claims.contracts.v1.CoverageResult;
import claims.contracts.v1.DispatchResult;
import claims.contracts.v1.EstimateResult;
import claims.contracts.v1.StepRequest;
import claims.stp.activities.CommHubActivities;
import claims.stp.activities.EClaimsActivities;
import claims.stp.activities.PolicyGateActivities;
import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Mock Comm Hub, Policy Gate and eClaims over the mock schema. Replace each with the real service
 * calls; the interfaces stay.
 */
public final class MockStpSystemsActivities {
  private MockStpSystemsActivities() {}

  public static class CommHub implements CommHubActivities {
    private final MockSystems mock;

    public CommHub(MockSystems mock) {
      this.mock = mock;
    }

    @Override
    public ContactResult contactCustomer(StepRequest request) {
      mock.maybeFail("Comm Hub");
      String claimId = request.getStep().getClaimId();
      int unreachable = mock.behaviour(claimId, "commHub").path("unreachable_attempts").asInt(0);
      try (Connection c = mock.connection();
          PreparedStatement ins = c.prepareStatement(
              "INSERT INTO mock.contacts (idempotency_key, claim_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
        ins.setString(1, MockSystems.key(request.getStep()));
        ins.setString(2, claimId);
        ins.executeUpdate();
        return ContactResult.newBuilder().setReached(MockSystems.attempt(request.getStep()) > unreachable).build();
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public static class PolicyGate implements PolicyGateActivities {
    private final MockSystems mock;

    public PolicyGate(MockSystems mock) {
      this.mock = mock;
    }

    @Override
    public CoverageResult verifyCoverage(StepRequest request) {
      mock.maybeFail("Policy Gate");
      JsonNode b = mock.behaviour(request.getStep().getClaimId(), "policyGate");
      return CoverageResult.newBuilder()
          .setCovered(b.path("covered").asBoolean(true))
          .setReason(b.path("reason").asText("")).build();
    }
  }

  public static class EClaims implements EClaimsActivities {
    private final MockSystems mock;

    public EClaims(MockSystems mock) {
      this.mock = mock;
    }

    /** Vendors decline the first N dispatches; on acceptance the estimate is scheduled in eClaims. */
    @Override
    public DispatchResult dispatchVendor(StepRequest request) {
      mock.maybeFail("eClaims");
      String claimId = request.getStep().getClaimId();
      JsonNode b = mock.behaviour(claimId, "eClaims");
      try (Connection c = mock.connection()) {
        c.setAutoCommit(false);
        try (PreparedStatement ins = c.prepareStatement(
            "INSERT INTO mock.dispatches (idempotency_key, claim_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
          ins.setString(1, MockSystems.key(request.getStep()));
          ins.setString(2, claimId);
          ins.executeUpdate();
        }
        int attempt = MockSystems.attempt(request.getStep());
        boolean accepted = attempt > b.path("no_vendor_attempts").asInt(0);
        String vendor = accepted ? "VENDOR-" + (100 + attempt) : "";
        try (PreparedStatement upd = c.prepareStatement(
            "UPDATE mock.dispatches SET accepted = ?, vendor_id = ? WHERE idempotency_key = ?")) {
          upd.setBoolean(1, accepted);
          upd.setString(2, vendor);
          upd.setString(3, MockSystems.key(request.getStep()));
          upd.executeUpdate();
        }
        if (accepted) {
          scheduleEstimate(c, claimId, b.path("estimate"));
        }
        c.commit();
        return DispatchResult.newBuilder().setAccepted(accepted).setVendorId(vendor).build();
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }

    /** The vendor's estimate arrives after a delay; eClaims' change is picked up by the event bridge unless dropped. */
    private void scheduleEstimate(Connection c, String claimId, JsonNode e) throws SQLException {
      try (PreparedStatement ps = c.prepareStatement("""
              INSERT INTO mock.estimates (claim_id, amount_cents, available_at)
              VALUES (?, ?, now() + make_interval(secs => ?)) ON CONFLICT (claim_id) DO NOTHING""")) {
        ps.setString(1, claimId);
        ps.setLong(2, e.path("amount_cents").asLong(250_000));
        ps.setInt(3, e.path("delay_seconds").asInt(5));
        ps.executeUpdate();
      }
      if (!e.path("drop_event").asBoolean(false)) {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO mock.change_events (event_id, claim_id, type, source_system, visible_at)
                VALUES (?, ?, 'ESTIMATE_RECEIVED', 'eClaims', now() + make_interval(secs => ?))
                ON CONFLICT (event_id) DO NOTHING""")) {
          ps.setString(1, "eclaims-estimate-" + claimId);
          ps.setString(2, claimId);
          ps.setInt(3, e.path("delay_seconds").asInt(5));
          ps.executeUpdate();
        }
      }
    }

    @Override
    public AppraisalResult assignAppraisal(StepRequest request) {
      mock.maybeFail("eClaims");
      String sql = """
          INSERT INTO mock.appraisals (idempotency_key, claim_id) VALUES (?, ?)
          ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = excluded.idempotency_key
          RETURNING appraisal_id""";
      try (Connection c = mock.connection(); PreparedStatement ps = c.prepareStatement(sql)) {
        ps.setString(1, MockSystems.key(request.getStep()));
        ps.setString(2, request.getStep().getClaimId());
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          return AppraisalResult.newBuilder().setAppraisalId(rs.getString(1)).build();
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public EstimateResult getEstimate(ClaimRef ref) {
      mock.maybeFail("eClaims");
      try (Connection c = mock.connection();
          PreparedStatement ps = c.prepareStatement(
              "SELECT amount_cents FROM mock.estimates WHERE claim_id = ? AND available_at <= now()")) {
        ps.setString(1, ref.getClaimId());
        try (ResultSet rs = ps.executeQuery()) {
          return rs.next()
              ? EstimateResult.newBuilder().setAvailable(true).setAmountCents(rs.getLong(1)).build()
              : EstimateResult.newBuilder().setAvailable(false).build();
        }
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
