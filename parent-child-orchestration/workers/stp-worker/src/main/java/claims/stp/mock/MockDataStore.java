package claims.stp.mock;

import claims.runtime.Env;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import javax.sql.DataSource;

/**
 * Mock application data: the "mock" schema in Postgres (mock-data/mock-schema.sql), seeded from the scenario
 * files in mock-data/scenarios. Each claim's "behaviour" says how the mocked systems respond. Also injects
 * random transient failures (MOCK_FAILURE_RATE) so retries are exercised.
 */
public final class MockDataStore {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final DataSource db;
  private final double failureRate = Env.getDouble("MOCK_FAILURE_RATE", 0.0);

  public MockDataStore(DataSource db) {
    this.db = db;
  }

  public Connection connection() throws SQLException {
    return db.getConnection();
  }

  /** Throws a retryable error at random, before any work is done. */
  public void maybeFail(String system) {
    if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
      throw new IllegalStateException(system + " unavailable (simulated)");
    }
  }

  public JsonNode behaviour(String claimId, String system) {
    try (Connection c = db.getConnection();
        PreparedStatement ps = c.prepareStatement("SELECT behaviour FROM mock.claims WHERE claim_id = ?")) {
      ps.setString(1, claimId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? JSON.readTree(rs.getString(1)).path(system) : JSON.createObjectNode();
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Attempt number from a step name such as stp.dispatch.2. */
  public static int attempt(claims.contracts.StepRef s) {
    String step = s.getStep();
    try {
      return Integer.parseInt(step.substring(step.lastIndexOf('.') + 1));
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  public static String key(claims.contracts.StepRef s) {
    return s.getClaimId() + ":" + s.getStageVersion() + ":" + s.getStep();
  }
}
