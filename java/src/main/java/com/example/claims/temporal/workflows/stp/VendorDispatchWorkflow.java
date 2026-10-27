package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.Names;
import com.example.claims.temporal.activities.StepContext;
import com.example.claims.temporal.activities.StepResult;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Grandchild of the lifecycle (child of STP). A reusable unit with its own history: STP, a future
 * non-STP flow, or an adjuster tool can all start it.
 */
@WorkflowInterface
public interface VendorDispatchWorkflow {
  @WorkflowMethod(name = Names.VENDOR_DISPATCH_WORKFLOW)
  StepResult run(StepContext ctx);
}
