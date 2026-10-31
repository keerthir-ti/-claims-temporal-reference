package claims.lifecycle.workflows;

import claims.contracts.v1.ArchiveReport;
import claims.contracts.v1.ArchiveRequest;
import claims.runtime.Names;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Nightly, by schedule: archives business history to S3 and makes sure every closed history reached S3. */
@WorkflowInterface
public interface ArchiveWorkflow {
  @WorkflowMethod(name = Names.ARCHIVE)
  ArchiveReport run(ArchiveRequest request);
}
