package claims.lifecycle.activities;

import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.Done;
import claims.contracts.v1.ProjectionEvent;
import claims.contracts.v1.RecordCheck;
import claims.contracts.v1.Stage;
import claims.contracts.v1.StageChange;
import claims.contracts.v1.StageInfo;
import claims.runtime.activities.ProjectionActivities;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.failure.ApplicationFailure;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/** The projection table (business history). Real implementation: keep it. */
public class ProjectionActivitiesImpl implements ProjectionActivities {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Done DONE = Done.newBuilder().setOk(true).build();
  private final DataSource db;

  public ProjectionActivitiesImpl(DataSource db) {
    this.db = db;
  }

  @Override
  public Done record(ProjectionEvent e) {
    String sql = """
        INSERT INTO projection.claim_events
          (claim_id, workflow_id, run_id, workflow_type, seq, stage_version, step, outcome, trigger_id,
           actor, reason, refs, closing, occurred_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
        ON CONFLICT DO NOTHING""";
    try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, e.getClaimId());
      ps.setString(2, e.getWorkflowId());
      ps.setString(3, e.getRunId());
      ps.setString(4, e.getWorkflowType());
      ps.setInt(5, e.getSeq());
      ps.setInt(6, e.getStageVersion());
      ps.setString(7, e.getStep());
      ps.setString(8, e.getOutcome());
      ps.setString(9, e.getTriggerId());
      ps.setString(10, e.getActor());
      ps.setString(11, e.getReason());
      ps.setString(12, JSON.writeValueAsString(e.getRefsMap()));
      ps.setBoolean(13, e.getClosing());
      ps.setTimestamp(14, new Timestamp(e.getOccurredAtMs()));
      ps.executeUpdate();
      return DONE;
    } catch (Exception ex) {
      throw new IllegalStateException("Projection write failed", ex); // retried until written
    }
  }

  @Override
  public Done beginStage(StageChange change) {
    String sql = """
        INSERT INTO projection.claim_stage (claim_id, stage_version, stage, updated_at)
        VALUES (?, ?, ?, now())
        ON CONFLICT (claim_id) DO UPDATE SET stage_version = excluded.stage_version, stage = excluded.stage,
          updated_at = now()
        WHERE projection.claim_stage.stage_version < excluded.stage_version""";
    try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, change.getStep().getClaimId());
      ps.setInt(2, change.getStep().getStageVersion());
      ps.setString(3, change.getStage().name());
      ps.executeUpdate();
      return DONE;
    } catch (SQLException ex) {
      throw new IllegalStateException("Stage write failed", ex);
    }
  }

  @Override
  public StageInfo currentStage(ClaimRef ref) {
    try (Connection c = db.getConnection();
        PreparedStatement ps = c.prepareStatement(
            "SELECT stage_version, stage FROM projection.claim_stage WHERE claim_id = ?")) {
      ps.setString(1, ref.getClaimId());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return StageInfo.getDefaultInstance();
        }
        return StageInfo.newBuilder().setStageVersion(rs.getInt(1)).setStage(Stage.valueOf(rs.getString(2))).build();
      }
    } catch (SQLException ex) {
      throw new IllegalStateException("Stage read failed", ex);
    }
  }

  /** Every other run of the claim has rows 1..n and a closing row; the current run has rows 1..current_seq. */
  @Override
  public Done verifyRecord(RecordCheck check) {
    String sql = """
        SELECT workflow_id, run_id, count(*), max(seq), bool_or(closing)
        FROM projection.claim_events WHERE claim_id = ? GROUP BY workflow_id, run_id""";
    List<String> problems = new ArrayList<>();
    try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, check.getClaimId());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String run = rs.getString(1) + "/" + rs.getString(2);
          int count = rs.getInt(3);
          int max = rs.getInt(4);
          boolean closed = rs.getBoolean(5);
          if (count != max) {
            problems.add(run + " has " + count + " of " + max + " rows");
          }
          if (rs.getString(2).equals(check.getCurrentRunId())) {
            if (max != check.getCurrentSeq()) {
              problems.add(run + " has " + max + " of " + check.getCurrentSeq() + " rows");
            }
          } else if (!closed) {
            problems.add(run + " has no closing row (still running?)");
          }
        }
      }
    } catch (SQLException ex) {
      throw new IllegalStateException("Record check failed", ex);
    }
    if (!problems.isEmpty()) {
      throw ApplicationFailure.newFailure("Incomplete business record: " + String.join("; ", problems), "IncompleteRecord");
    }
    return DONE;
  }
}
