package com.example.claims.temporal.activities;

import com.example.claims.temporal.Names;
import com.example.claims.contracts.ProjectionRecord;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Writes the application-owned business projection. Hosted on {@link Names#PROJECTION_QUEUE} and
 * called by name from Java and Python workflows alike.
 */
@ActivityInterface
public interface ProjectionActivities {
  @ActivityMethod(name = Names.ACTIVITY_RECORD_PROJECTION)
  void recordProjection(ProjectionRecord record);
}
