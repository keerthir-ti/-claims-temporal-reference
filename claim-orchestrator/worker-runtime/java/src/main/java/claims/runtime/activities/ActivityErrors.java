package claims.runtime.activities;

import io.temporal.failure.ApplicationFailure;
import java.util.Set;

/**
 * Business failures an activity reports instead of retrying. Workflows take the business path for
 * these; every other failure is retried until the step's retry window ends.
 */
public final class ActivityErrors {
  private ActivityErrors() {}

  public static final String NOT_FOUND = "NotFound";
  public static final String REFUSED = "Refused";
  public static final String STALE_STAGE = "StaleStage";
  public static final Set<String> BUSINESS = Set.of(NOT_FOUND, REFUSED, STALE_STAGE);

  public static ApplicationFailure notFound(String message) {
    return ApplicationFailure.newNonRetryableFailure(message, NOT_FOUND);
  }

  public static ApplicationFailure refused(String message) {
    return ApplicationFailure.newNonRetryableFailure(message, REFUSED);
  }

  public static ApplicationFailure staleStage(String message) {
    return ApplicationFailure.newNonRetryableFailure(message, STALE_STAGE);
  }
}
