package claims.lifecycle.workflows;

import static claims.contracts.Stage.STAGE_CLOSED;
import static claims.contracts.Stage.STAGE_ESCALATION;
import static claims.contracts.Stage.STAGE_INVESTIGATION;
import static claims.contracts.Stage.STAGE_MANUAL;
import static claims.contracts.Stage.STAGE_REOPEN_REVIEW;
import static claims.contracts.Stage.STAGE_SIU;
import static claims.contracts.Stage.STAGE_STP;
import static claims.contracts.Stage.STAGE_TRIAGE;

import claims.contracts.Stage;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackResult;

/**
 * The whole claim lifecycle as pure functions: the only place that knows how tracks and human stages
 * connect. Tracks report an outcome; they never choose the next track themselves. Anything this table does
 * not know goes to MANUAL. Changing a route is a code change, patched and replay-tested.
 */
public final class Routing {
  private Routing() {}

  /** Next stage; {@code closeInClaimsPro} when the claim must be closed in ClaimsPro first. */
  public record Next(Stage stage, String reason, boolean retriage, boolean closeInClaimsPro) {
    static Next to(Stage stage, String reason) {
      return new Next(stage, reason, false, false);
    }

    static Next retriage(String reason) {
      return new Next(STAGE_TRIAGE, reason, true, false);
    }

    static Next close(String reason) {
      return new Next(STAGE_CLOSED, reason, false, true);
    }
  }

  public static Next afterTrack(Stage from, TrackResult r) {
    String reason = r.getReason();
    TrackOutcome o = r.getOutcome();
    switch (from) {
      case STAGE_TRIAGE:
        return switch (o) {
          case ROUTE_STP -> Next.to(STAGE_STP, reason);
          case ROUTE_INVESTIGATION -> Next.to(STAGE_INVESTIGATION, reason);
          case ROUTE_SIU -> Next.to(STAGE_SIU, reason);
          case ROUTE_MANUAL -> Next.to(STAGE_MANUAL, reason);
          default -> Next.to(STAGE_MANUAL, "triage outcome not allowed: " + o);
        };
      case STAGE_INVESTIGATION:
        return switch (o) {
          case RETRIAGE -> Next.retriage(reason);
          case ROUTE_MANUAL -> Next.to(STAGE_MANUAL, reason);
          default -> Next.to(STAGE_MANUAL, "investigation outcome not allowed: " + o);
        };
      case STAGE_STP:
        return switch (o) {
          case COMPLETED -> Next.to(STAGE_CLOSED, reason);
          case RETRIAGE -> Next.retriage(reason);
          case ROUTE_MANUAL -> Next.to(STAGE_MANUAL, reason);
          default -> Next.to(STAGE_MANUAL, "STP outcome not allowed: " + o);
        };
      default:
        throw new IllegalArgumentException("Not a track: " + from);
    }
  }

  public static Next afterDecision(Stage stage, String choice) {
    String deadline = "deadline passed in " + stage.name();
    return switch (stage) {
      case STAGE_MANUAL -> switch (choice) {
        case "CLOSE" -> Next.close("closed by adjuster");
        case "RETURN_TO_STP" -> Next.to(STAGE_STP, "returned to STP by adjuster");
        case "RETRIAGE" -> Next.retriage("re-triage asked by adjuster");
        case "REFER_SIU" -> Next.to(STAGE_SIU, "referred to SIU by adjuster");
        default -> Next.to(STAGE_ESCALATION, deadline);
      };
      case STAGE_ESCALATION -> switch (choice) {
        case "REASSIGN" -> Next.to(STAGE_MANUAL, "reassigned by supervisor");
        case "CLOSE" -> Next.close("closed by supervisor");
        default -> Next.to(STAGE_ESCALATION, deadline);
      };
      case STAGE_SIU -> switch (choice) {
        case "CLEAR" -> Next.to(STAGE_TRIAGE, "cleared by SIU");
        case "DENY" -> Next.close("denied by SIU");
        default -> Next.to(STAGE_SIU, deadline);
      };
      case STAGE_REOPEN_REVIEW -> switch (choice) {
        case "APPROVE" -> Next.to(STAGE_TRIAGE, "reopen approved");
        case "REJECT" -> Next.close("reopen rejected");
        default -> Next.to(STAGE_REOPEN_REVIEW, deadline);
      };
      default -> throw new IllegalArgumentException("No decision in " + stage);
    };
  }

  /** Choices offered in each human stage. */
  public static java.util.List<String> choices(Stage stage) {
    return switch (stage) {
      case STAGE_MANUAL -> java.util.List.of("CLOSE", "RETURN_TO_STP", "RETRIAGE", "REFER_SIU");
      case STAGE_ESCALATION -> java.util.List.of("REASSIGN", "CLOSE");
      case STAGE_SIU -> java.util.List.of("CLEAR", "DENY");
      case STAGE_REOPEN_REVIEW -> java.util.List.of("APPROVE", "REJECT");
      default -> throw new IllegalArgumentException("No decision in " + stage);
    };
  }

  public static claims.contracts.DecisionKind decisionKind(Stage stage) {
    return switch (stage) {
      case STAGE_MANUAL -> claims.contracts.DecisionKind.MANUAL_HANDLING;
      case STAGE_ESCALATION -> claims.contracts.DecisionKind.ESCALATION;
      case STAGE_SIU -> claims.contracts.DecisionKind.SIU_REVIEW;
      case STAGE_REOPEN_REVIEW -> claims.contracts.DecisionKind.REOPEN_REVIEW;
      default -> throw new IllegalArgumentException("No decision in " + stage);
    };
  }
}
