package claims.runtime.workflow;

import claims.contracts.v1.ProjectionEvent;
import claims.runtime.activities.ProjectionActivities;
import io.temporal.workflow.Workflow;
import java.util.Map;

/**
 * Writes the business history of one workflow run: one row per step, decision and event acted on,
 * numbered 1..n with no gaps, ending with a closing row. Each write is awaited.
 */
public final class Recorder {
  private final String claimId;
  private final String workflowType;
  private final ProjectionActivities projection = Stubs.projection();
  private int seq;

  public Recorder(String claimId, String workflowType) {
    this.claimId = claimId;
    this.workflowType = workflowType;
  }

  public Row row(int stageVersion, String step) {
    return new Row(stageVersion, step);
  }

  public void closing(int stageVersion, String outcome) {
    new Row(stageVersion, "run.closed").outcome(outcome).closing().write();
  }

  public int lastSeq() {
    return seq;
  }

  public final class Row {
    private final ProjectionEvent.Builder b = ProjectionEvent.newBuilder();

    private Row(int stageVersion, String step) {
      b.setClaimId(claimId).setWorkflowType(workflowType).setStageVersion(stageVersion).setStep(step);
    }

    public Row outcome(String v) { b.setOutcome(nz(v)); return this; }
    public Row trigger(String v) { b.setTriggerId(nz(v)); return this; }
    public Row actor(String v) { b.setActor(nz(v)); return this; }
    public Row reason(String v) { b.setReason(nz(v)); return this; }
    public Row ref(String k, String v) { b.putRefs(k, nz(v)); return this; }
    public Row refs(Map<String, String> m) { b.putAllRefs(m); return this; }
    Row closing() { b.setClosing(true); return this; }

    public void write() {
      seq++;
      var info = Workflow.getInfo();
      projection.record(b.setWorkflowId(info.getWorkflowId()).setRunId(info.getRunId()).setSeq(seq)
          .setOccurredAtMs(Workflow.currentTimeMillis()).build());
    }
  }

  private static String nz(String v) {
    return v == null ? "" : v;
  }
}
