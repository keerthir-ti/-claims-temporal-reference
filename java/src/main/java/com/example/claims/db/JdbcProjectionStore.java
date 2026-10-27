package com.example.claims.db;

import com.example.claims.contracts.ProjectionRecord;
import com.example.claims.util.Env;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Postgres projection (schema: db/postgres/init.sql). Pooled: under a CAT surge every claim writes
 * many rows, and opening a connection per write would exhaust Postgres.
 */
public class JdbcProjectionStore implements ProjectionStore {
  private static final String SQL =
      "INSERT INTO claim_projection (projection_key, claim_id, stage_version, track, step, status,"
          + " detail, workflow_id, run_id, occurred_at) VALUES (?,?,?,?,?,?,?,?,?,?)"
          + " ON CONFLICT (projection_key) DO NOTHING";

  private final HikariDataSource pool;

  public JdbcProjectionStore(String url, String user, String password) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);
    config.setPoolName("projection");
    config.setMaximumPoolSize(Env.getInt("PROJECTION_DB_POOL_SIZE", 10));
    // Do not fail worker startup if the database is briefly down: writes retry through Temporal.
    config.setInitializationFailTimeout(-1);
    this.pool = new HikariDataSource(config);
  }

  @Override
  public boolean insertIfAbsent(ProjectionRecord r) {
    try (Connection c = pool.getConnection();
        PreparedStatement ps = c.prepareStatement(SQL)) {
      ps.setString(1, ProjectionStore.key(r));
      ps.setString(2, r.getClaimId());
      ps.setInt(3, r.getStageVersion());
      ps.setString(4, r.getTrack() == null ? null : r.getTrack().value());
      ps.setString(5, r.getStep());
      ps.setString(6, r.getStatus());
      ps.setString(7, r.getDetail());
      ps.setString(8, r.getWorkflowId());
      ps.setString(9, r.getRunId());
      ps.setTimestamp(10, Timestamp.from(r.getOccurredAt() == null ? Instant.now() : Instant.parse(r.getOccurredAt())));
      return ps.executeUpdate() == 1;
    } catch (SQLException e) {
      // Retryable: the activity RetryPolicy keeps trying while the database is unavailable.
      throw new IllegalStateException("projection write failed: " + e.getMessage(), e);
    }
  }
}
