package claims.runtime.workflow;

import claims.contracts.v1.DecisionKind;
import claims.contracts.v1.DecisionResult;
import claims.runtime.activities.ActivityErrors;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.CancellationScope;
import java.util.List;
import java.util.function.Supplier;

/**
 * Runs a step whose activity retries within a retry window. Business failures go back to the caller.
 * When the window ends, the workflow opens an operations task and waits: RETRY runs the step again
 * with the same idempotency key, STOP throws {@link Stopped} so the workflow hands the claim to a person.
 * An outage of a system delays claims but never fails them.
 */
public final class Steps {
  private final DecisionDesk desk;

  public Steps(DecisionDesk desk) {
    this.desk = desk;
  }

  public <T> T run(String step, Supplier<T> call) {
    while (true) {
      try {
        return call.get();
      } catch (ActivityFailure f) {
        if (isBusiness(f) || CancellationScope.current().isCancelRequested()) {
          throw f;
        }
        DecisionResult r = desk.decide(DecisionKind.OPERATIONS_EXCEPTION, List.of("RETRY", "STOP"),
            step + " failed after its retry window: " + rootMessage(f), 0, "", () -> false);
        if (!"RETRY".equals(r.getChoice()) && !DecisionDesk.DEADLINE_PASSED.equals(r.getChoice())) {
          throw new Stopped(step);
        }
      }
    }
  }

  public static boolean isBusiness(ActivityFailure f) {
    return f.getCause() instanceof ApplicationFailure af
        && (af.isNonRetryable() || ActivityErrors.BUSINESS.contains(af.getType()));
  }

  public static String businessType(ActivityFailure f) {
    return f.getCause() instanceof ApplicationFailure af ? af.getType() : "";
  }

  private static String rootMessage(Throwable t) {
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t.getMessage();
  }

  /** A person chose to stop retrying a step. */
  public static final class Stopped extends RuntimeException {
    public Stopped(String step) {
      super("Stopped at " + step);
    }
  }
}
