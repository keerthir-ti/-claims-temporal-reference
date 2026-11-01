package claims.stp.mock;

import claims.contracts.ContactResult;
import claims.contracts.StepRequest;
import claims.runtime.activities.CommHubActivities;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** Mock Comm Hub over the mock schema. Replace with the real Comm Hub call; the interface stays. */
public class MockCommHubActivities implements CommHubActivities {
  private final MockDataStore mock;

  public MockCommHubActivities(MockDataStore mock) {
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
      ins.setString(1, MockDataStore.key(request.getStep()));
      ins.setString(2, claimId);
      ins.executeUpdate();
      return ContactResult.newBuilder().setReached(MockDataStore.attempt(request.getStep()) > unreachable).build();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
