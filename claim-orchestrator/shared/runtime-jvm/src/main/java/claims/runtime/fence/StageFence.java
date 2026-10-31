package claims.runtime.fence;

import claims.contracts.v1.StepRef;
import claims.runtime.activities.ActivityErrors;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Refuses a write when the claim has moved to a newer stage version than the step's. The current
 * version is in the projection table (projection.claim_stage), so ClaimsPro needs no change.
 */
public final class StageFence {
  private final DataSource projection;

  public StageFence(DataSource projection) {
    this.projection = projection;
  }

  public void check(StepRef step) {
    try (Connection c = projection.getConnection();
        PreparedStatement ps = c.prepareStatement(
            "SELECT stage_version FROM projection.claim_stage WHERE claim_id = ?")) {
      ps.setString(1, step.getClaimId());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next() && rs.getInt(1) > step.getStageVersion()) {
          throw ActivityErrors.staleStage("Claim " + step.getClaimId() + " is at stage version "
              + rs.getInt(1) + "; step " + step.getStep() + " belongs to version " + step.getStageVersion());
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Stage check failed", e); // retried
    }
  }
}
