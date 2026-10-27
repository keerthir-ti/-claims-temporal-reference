package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.activities.StepContext;
import com.example.claims.temporal.activities.StepResult;
import com.example.claims.temporal.activities.StpActivities;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.List;

public class VendorDispatchWorkflowImpl implements VendorDispatchWorkflow {
  private final StpActivities activities = Workflow.newActivityStub(StpActivities.class,
      ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(30)).build());

  @Override
  public StepResult run(StepContext ctx) {
    List<String> vendors = activities.listVendors(ctx);
    for (String vendorId : vendors) {
      StepResult offer = activities.offerDispatch(ctx, vendorId);
      if (offer.isOk()) {
        return StepResult.ok("dispatched to " + vendorId, vendorId, null);
      }
      Workflow.getLogger(VendorDispatchWorkflowImpl.class).info("Vendor {} declined", vendorId);
    }
    return StepResult.handoff("no vendor accepted dispatch (" + vendors.size() + " offered)");
  }
}
