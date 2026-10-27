package com.example.claims.temporal.client;

import com.example.claims.util.Env;
import com.sun.net.httpserver.HttpServer;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.serviceclient.SimpleSslContextBuilder;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the Temporal client from env vars. Works unchanged against the local dev server and a
 * Temporal Cloud namespace (API key or mTLS), and exposes SDK metrics on a Prometheus endpoint
 * that Metricbeat ships into Elasticsearch.
 *
 * <pre>
 * TEMPORAL_ADDRESS     localhost:7233 | ns.acct.tmprl.cloud:7233
 * TEMPORAL_NAMESPACE   default        | ns.acct
 * TEMPORAL_API_KEY     (Cloud API key auth)
 * TEMPORAL_TLS_CERT / TEMPORAL_TLS_KEY   (Cloud mTLS, PEM / PKCS8)
 * METRICS_PORT         9464
 * </pre>
 */
public final class TemporalSupport {
  private static final Logger log = LoggerFactory.getLogger(TemporalSupport.class);

  private TemporalSupport() {}

  public static WorkflowClient connect() {
    String address = Env.get("TEMPORAL_ADDRESS", "localhost:7233");
    String namespace = Env.get("TEMPORAL_NAMESPACE", "default");
    WorkflowServiceStubsOptions.Builder stubs =
        WorkflowServiceStubsOptions.newBuilder().setTarget(address).setMetricsScope(metricsScope());

    String apiKey = Env.get("TEMPORAL_API_KEY", "");
    String cert = Env.get("TEMPORAL_TLS_CERT", "");
    if (!apiKey.isEmpty()) {
      stubs.addApiKey(() -> apiKey).setEnableHttps(true);
    } else if (!cert.isEmpty()) {
      try (InputStream c = new FileInputStream(cert);
          InputStream k = new FileInputStream(Env.get("TEMPORAL_TLS_KEY", ""))) {
        stubs.setSslContext(SimpleSslContextBuilder.forPKCS8(c, k).build());
      } catch (Exception e) {
        throw new IllegalStateException("Could not load mTLS cert/key", e);
      }
    }

    WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubs.build());
    log.info("Connected to Temporal address={} namespace={}", address, namespace);
    return WorkflowClient.newInstance(
        service,
        WorkflowClientOptions.newBuilder()
            .setNamespace(namespace)
            .setDataConverter(ContractJson.dataConverter())
            .build());
  }

  private static Scope metricsScope() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    int port = Env.getInt("METRICS_PORT", 9464);
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
      server.createContext("/metrics", exchange -> {
        byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      });
      server.start();
      log.info("Prometheus metrics on :{}/metrics", port);
    } catch (Exception e) {
      log.warn("Metrics endpoint disabled: {}", e.toString());
    }
    return new RootScopeBuilder()
        .reporter(new MicrometerClientStatsReporter(registry))
        .reportEvery(com.uber.m3.util.Duration.ofSeconds(10));
  }
}
