package claims.lifecycle.activities;

import claims.contracts.v1.ArchiveReport;
import claims.contracts.v1.ArchiveRequest;
import claims.lifecycle.archive.ArchiveStore;
import claims.runtime.Env;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.temporal.activity.Activity;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionMetadata;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Archive logic. Real implementation: keep it; only the {@link ArchiveStore} changes for S3. */
public class ArchiveActivitiesImpl implements ArchiveActivities {
  private static final Logger log = LoggerFactory.getLogger(ArchiveActivitiesImpl.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private final DataSource db;
  private final ArchiveStore store;
  private final WorkflowClient client;

  public ArchiveActivitiesImpl(DataSource db, ArchiveStore store, WorkflowClient client) {
    this.db = db;
    this.store = store;
    this.client = client;
  }

  @Override
  public ArchiveReport archiveClosedClaims(ArchiveRequest request) {
    Instant cutoff = Instant.now().minus(request.getClosedMoreThanDays(), ChronoUnit.DAYS);
    int claims = 0;
    int rows = 0;
    for (String[] claim : closedClaims(cutoff)) {
      Activity.getExecutionContext().heartbeat(claim[0]);
      int n = archiveClaim(claim[0], claim[1]);
      if (n > 0) {
        claims++;
        rows += n;
      }
    }
    log.info("Archived {} rows of {} claims closed before {}", rows, claims, cutoff);
    return ArchiveReport.newBuilder().setClaims(claims).setRows(rows).build();
  }

  private List<String[]> closedClaims(Instant cutoff) {
    String sql = """
        SELECT s.claim_id, to_char(s.updated_at, 'YYYY-MM-DD') FROM projection.claim_stage s
        WHERE s.stage = 'STAGE_CLOSED' AND s.updated_at < ?
          AND EXISTS (SELECT 1 FROM projection.claim_events e WHERE e.claim_id = s.claim_id)""";
    List<String[]> out = new ArrayList<>();
    try (Connection c = db.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setTimestamp(1, Timestamp.from(cutoff));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(new String[] {rs.getString(1), rs.getString(2)});
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return out;
  }

  /** Copy, verify (row count and checksum), and only then delete. A rerun overwrites the same object. */
  private int archiveClaim(String claimId, String closeDate) {
    try (Connection c = db.getConnection()) {
      c.setAutoCommit(false);
      StringBuilder lines = new StringBuilder();
      int count = 0;
      try (PreparedStatement ps = c.prepareStatement(
          "SELECT * FROM projection.claim_events WHERE claim_id = ? ORDER BY occurred_at, run_id, seq FOR UPDATE")) {
        ps.setString(1, claimId);
        try (ResultSet rs = ps.executeQuery()) {
          ResultSetMetaData md = rs.getMetaData();
          while (rs.next()) {
            ObjectNode row = JSON.createObjectNode();
            for (int i = 1; i <= md.getColumnCount(); i++) {
              Object v = rs.getObject(i);
              if (md.getColumnTypeName(i).equals("jsonb")) {
                row.set(md.getColumnName(i), JSON.readTree(String.valueOf(v)));
              } else {
                row.put(md.getColumnName(i), v == null ? null : String.valueOf(v));
              }
            }
            lines.append(JSON.writeValueAsString(row)).append('\n');
            count++;
          }
        }
      }
      if (count == 0) {
        c.rollback();
        return 0;
      }
      byte[] data = lines.toString().getBytes(StandardCharsets.UTF_8);
      String key = "business/close_date=" + closeDate + "/" + claimId + ".jsonl";
      store.put(key, data);
      byte[] back = store.get(key);
      if (!sha256(back).equals(sha256(data)) || lineCount(back) != count) {
        c.rollback();
        throw new IllegalStateException("Archive verification failed for " + key); // retried
      }
      try (PreparedStatement del = c.prepareStatement("DELETE FROM projection.claim_events WHERE claim_id = ?")) {
        del.setString(1, claimId);
        del.executeUpdate();
      }
      c.commit();
      return count;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Archive of " + claimId + " failed", e);
    }
  }

  @Override
  public ArchiveReport checkHistoryExports(ArchiveRequest request) {
    int hours = Env.getInt("HISTORY_CHECK_HOURS", 48);
    String since = Instant.now().minus(hours, ChronoUnit.HOURS).toString();
    String query = "ExecutionStatus != 'Running' AND CloseTime > '" + since + "'";
    int checked = 0;
    int written = 0;
    try (Stream<WorkflowExecutionMetadata> closed = client.listExecutions(query)) {
      for (WorkflowExecutionMetadata wf : (Iterable<WorkflowExecutionMetadata>) closed::iterator) {
        String workflowId = wf.getExecution().getWorkflowId();
        String runId = wf.getExecution().getRunId();
        String key = store.historyKey(workflowId, runId);
        checked++;
        Activity.getExecutionContext().heartbeat(checked);
        if (!store.exists(key)) {
          // Not exported (yet): copy it while Temporal still retains it.
          store.put(key, client.fetchHistory(workflowId, runId).toJson(true).getBytes(StandardCharsets.UTF_8));
          written++;
        }
      }
    }
    log.info("History check: {} closed workflows, {} written by the fallback", checked, written);
    return ArchiveReport.newBuilder().setHistoriesChecked(checked).setHistoriesWritten(written).build();
  }

  private static String sha256(byte[] data) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
  }

  private static int lineCount(byte[] data) {
    int n = 0;
    for (byte b : data) {
      if (b == '\n') {
        n++;
      }
    }
    return n;
  }
}
