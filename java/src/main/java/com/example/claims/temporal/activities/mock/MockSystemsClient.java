package com.example.claims.temporal.activities.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.claims.temporal.activities.ActivityErrors;
import com.example.claims.temporal.activities.StepContext;
import com.example.claims.temporal.client.ContractJson;
import com.example.claims.util.Env;
import io.temporal.failure.ApplicationFailure;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * MOCK ONLY. HTTP client for the mock-data service, which holds the scripted claim data and stands
 * in for ClaimsPro, Policy Gate, Comm Hub and eClaims (one path prefix per system).
 *
 * <p>Two things here are NOT mock-specific and must survive in the real activity code:
 * <ul>
 *   <li>every write sends {@code X-Stage-Version} (the system rejects stale stages) and
 *       {@code Idempotency-Key} (a repeat returns the original result instead of acting twice);</li>
 *   <li>the error mapping decides what Temporal retries: 404 / 422 / 409 are non-retryable,
 *       5xx and network errors are thrown as-is and retried by the activity's RetryPolicy.</li>
 * </ul>
 */
public class MockSystemsClient {
  private static final ObjectMapper JSON = ContractJson.MAPPER;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final String baseUrl;

  public MockSystemsClient() {
    this(Env.get("MOCK_SYSTEMS_URL", "http://localhost:8090"));
  }

  public MockSystemsClient(String baseUrl) {
    this.baseUrl = baseUrl.replaceAll("/$", "");
  }

  public JsonNode get(String path) {
    return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET(), null);
  }

  public JsonNode post(String path, Object body) {
    return post(path, body, null);
  }

  public JsonNode post(String path, Object body, StepContext ctx) {
    try {
      HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(baseUrl + path))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body == null ? Map.of() : body)));
      return send(req, ctx);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private JsonNode send(HttpRequest.Builder req, StepContext ctx) {
    req.timeout(Duration.ofSeconds(10));
    if (ctx != null) {
      req.header("X-Stage-Version", Integer.toString(ctx.stageVersion()));
      req.header("Idempotency-Key", ctx.idempotencyKey());
    }
    HttpResponse<String> resp;
    try {
      resp = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted calling mock systems", e);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("mock systems unreachable: " + e.getMessage(), e);
    }
    int code = resp.statusCode();
    if (code == 404) {
      throw ApplicationFailure.newNonRetryableFailure(resp.body(), ActivityErrors.NOT_FOUND);
    }
    if (code == 409) {
      throw ApplicationFailure.newNonRetryableFailure(resp.body(), ActivityErrors.STALE_STAGE);
    }
    if (code == 422) {
      throw ApplicationFailure.newNonRetryableFailure(resp.body(), ActivityErrors.BUSINESS_RULE);
    }
    if (code >= 300) {
      throw new IllegalStateException("HTTP " + code + ": " + resp.body());
    }
    try {
      return JSON.readTree(resp.body().isEmpty() ? "{}" : resp.body());
    } catch (java.io.IOException e) {
      throw new IllegalStateException("Bad JSON from mock systems", e);
    }
  }
}
