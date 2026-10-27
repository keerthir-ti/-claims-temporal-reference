package com.example.claims.temporal.client;

import com.example.claims.temporal.Names;
import com.example.claims.temporal.SearchAttrs;
import com.example.claims.util.Env;
import com.fasterxml.jackson.databind.JsonNode;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.ClaimEventType;
import com.example.claims.contracts.LifecycleInput;
import com.example.claims.contracts.LifecycleSettings;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.SearchAttributes;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Claims Event Bridge: ClaimsPro change events in, canonical non-PII Signals out.
 *
 * <p><b>Demo-grade transport.</b> Events arrive over HTTP and a failed Signal is only logged, so an
 * event can be lost (the workflows' backstop polls recover it). Production shape: consume the DB2
 * change feed from a durable log (Kafka / Kinesis / CDC table), Signal, then commit the offset only
 * after the Signal succeeded (at-least-once; workflows dedupe by eventId), and send undeliverable
 * events to a real dead-letter topic for reconciliation. The mapping logic below stays the same.
 *
 * <p>Deliberately dumb: allowlist + normalise + derive Workflow ID + Signal the parent. It does not
 * know which child is active; the parent decides relevance.
 * <ul>
 *   <li>FNOL and REOPEN use Signal-With-Start, so the lifecycle exists exactly once per claim and a
 *       claim whose workflow already completed gets a new run under the same Workflow ID.</li>
 *   <li>Every other event is a plain Signal. If no workflow is running it is NOT allowed to start
 *       one (an UPDATE for a closed claim must not resurrect it); it goes to the dead-letter log.</li>
 * </ul>
 */
public class EventBridge {
  private static final Logger log = LoggerFactory.getLogger(EventBridge.class);

  private static final Map<String, ClaimEventType> TYPE_MAP = Map.of(
      "claim.fnol", ClaimEventType.FNOL_RECEIVED,
      "claim.updated", ClaimEventType.CLAIM_UPDATED,
      "document.received", ClaimEventType.DOCUMENT_RECEIVED,
      "estimate.received", ClaimEventType.ESTIMATE_RECEIVED,
      "payment.cancel_requested", ClaimEventType.PAYMENT_CANCEL_REQUESTED,
      "claim.reopened", ClaimEventType.CLAIM_REOPENED);
  private static final Set<ClaimEventType> STARTERS =
      Set.of(ClaimEventType.FNOL_RECEIVED, ClaimEventType.CLAIM_REOPENED);
  /** Only these source attributes may cross into Temporal. Everything else (names, addresses...) is dropped. */
  private static final Set<String> ATTRIBUTE_ALLOWLIST = Set.of("changedFields", "documentType");

  private final WorkflowClient client;

  public EventBridge(WorkflowClient client) {
    this.client = client;
  }

  public void serve(int port) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(Executors.newFixedThreadPool(Env.getInt("BRIDGE_THREADS", 32)));
    server.createContext("/health", ex -> respond(ex, 200, "{\"ok\":true}"));
    server.createContext("/events", this::handle);
    server.start();
    log.info("Event bridge listening on :{}", port);
  }

  private void handle(HttpExchange ex) throws IOException {
    try {
      JsonNode raw = ContractJson.MAPPER.readTree(ex.getRequestBody());
      Result r = accept(raw);
      respond(ex, r.status, "{\"result\":\"" + r.message + "\"}");
    } catch (Exception e) {
      log.error("bridge error", e);
      respond(ex, 500, "{\"error\":\"" + e.getClass().getSimpleName() + "\"}");
    }
  }

  record Result(int status, String message) {}

  Result accept(JsonNode raw) {
    ClaimEventType type = TYPE_MAP.get(raw.path("type").asText());
    String claimId = raw.path("claimId").asText(null);
    String eventId = raw.path("id").asText(null);
    if (type == null || claimId == null || eventId == null) {
      log.info("Event not allowlisted or malformed, dropped: type={}", raw.path("type").asText());
      return new Result(202, "ignored");
    }
    ClaimEvent event = normalise(raw, type, claimId, eventId);
    String workflowId = Names.lifecycleWorkflowId(claimId);

    if (STARTERS.contains(type)) {
      WorkflowStub stub = client.newUntypedWorkflowStub(Names.LIFECYCLE_WORKFLOW,
          WorkflowOptions.newBuilder()
              .setWorkflowId(workflowId)
              .setTaskQueue(Names.LIFECYCLE_QUEUE)
              .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
              .setTypedSearchAttributes(SearchAttributes.newBuilder().set(SearchAttrs.CLAIM_ID, claimId).build())
              .build());
      LifecycleInput input = new LifecycleInput()
          .withSchemaVersion(1)
          .withClaimId(claimId)
          .withReopened(type == ClaimEventType.CLAIM_REOPENED)
          .withSettings(settingsFromEnv());
      stub.signalWithStart(Names.SIGNAL_CLAIM_EVENT, new Object[] {event}, new Object[] {input});
      log.info("Signal-With-Start {} {} -> {}", type, eventId, workflowId);
      return new Result(200, "signal-with-start");
    }

    try {
      client.newUntypedWorkflowStub(workflowId).signal(Names.SIGNAL_CLAIM_EVENT, event);
      log.info("Signalled {} {} -> {}", type, eventId, workflowId);
      return new Result(200, "signalled");
    } catch (WorkflowNotFoundException e) {
      // Dead letter: in production publish to a DLQ topic/table for reconciliation.
      log.warn("DEAD_LETTER no running lifecycle for claim {} event {} {}", claimId, type, eventId);
      return new Result(409, "dead-letter");
    }
  }

  static ClaimEvent normalise(JsonNode raw, ClaimEventType type, String claimId, String eventId) {
    Map<String, String> attributes = new LinkedHashMap<>();
    for (String key : ATTRIBUTE_ALLOWLIST) {
      JsonNode v = raw.get(key);
      if (v != null && !v.isNull()) {
        attributes.put(key, v.isArray() ? joinArray(v) : v.asText());
      }
    }
    return new ClaimEvent()
        .withSchemaVersion(1)
        .withEventId(eventId)
        .withClaimId(claimId)
        .withEventType(type)
        .withOccurredAt(raw.path("occurredAt").asText(null))
        .withSourceVersion(raw.has("version") ? raw.get("version").asInt() : null)
        .withAttributes(attributes);
  }

  static LifecycleSettings settingsFromEnv() {
    return new LifecycleSettings()
        .withTsaEnabled(Env.getBool("TSA_ENABLED", true))
        .withIsEnabled(Env.getBool("IS_ENABLED", true))
        .withMaxRetriage(Env.getInt("MAX_RETRIAGE", 3))
        .withManualSlaSeconds(Env.getInt("MANUAL_SLA_SECONDS", 300))
        .withReopenWindowSeconds(Env.getInt("REOPEN_WINDOW_SECONDS", 120))
        .withEventWaitSeconds(Env.getInt("EVENT_WAIT_SECONDS", 60))
        .withRevalidateIntervalSeconds(Env.getInt("REVALIDATE_INTERVAL_SECONDS", 15))
        .withContinueAsNewEventThreshold(Env.getInt("CAN_EVENT_THRESHOLD", 10000));
  }

  private static String joinArray(JsonNode arr) {
    StringBuilder sb = new StringBuilder();
    arr.forEach(n -> sb.append(sb.length() == 0 ? "" : ",").append(n.asText()));
    return sb.toString();
  }

  private static void respond(HttpExchange ex, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(code, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
