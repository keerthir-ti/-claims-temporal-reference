package claims.runtime.temporal;

import claims.runtime.Env;
import com.sun.net.httpserver.HttpServer;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.converter.CodecDataConverter;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.serviceclient.SimpleSslContextBuilder;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Temporal client from environment variables. The same settings work against the local dev server
 * and Temporal Cloud.
 *
 * <pre>
 * TEMPORAL_ADDRESS                     localhost:7233 | namespace.account.tmprl.cloud:7233
 * TEMPORAL_NAMESPACE                   default        | namespace.account
 * TEMPORAL_TLS_CERT, TEMPORAL_TLS_KEY  mTLS client certificate and PKCS8 key (Temporal Cloud)
 * PAYLOAD_KEY, PAYLOAD_KEY_ID          AES-256 key (base64) for payload encryption; off when unset
 * METRICS_PORT                         Prometheus endpoint for SDK metrics; 0 disables it
 * </pre>
 */
public final class TemporalConnection {
  private static final Logger log = LoggerFactory.getLogger(TemporalConnection.class);

  private TemporalConnection() {}

  public static WorkflowServiceStubs service() {
    String address = Env.get("TEMPORAL_ADDRESS", "localhost:7233");
    WorkflowServiceStubsOptions.Builder options = WorkflowServiceStubsOptions.newBuilder().setTarget(address);
    Scope metrics = metricsScope();
    if (metrics != null) {
      options.setMetricsScope(metrics);
    }
    String cert = Env.get("TEMPORAL_TLS_CERT", "");
    if (!cert.isEmpty()) {
      try (InputStream c = new FileInputStream(cert);
          InputStream k = new FileInputStream(Env.get("TEMPORAL_TLS_KEY", ""))) {
        options.setSslContext(SimpleSslContextBuilder.forPKCS8(c, k).build());
      } catch (Exception e) {
        throw new IllegalStateException("Could not load the mTLS certificate or key", e);
      }
    }
    log.info("Temporal at {} (mTLS {})", address, !cert.isEmpty());
    return WorkflowServiceStubs.newServiceStubs(options.build());
  }

  public static WorkflowClient client(WorkflowServiceStubs service) {
    return WorkflowClient.newInstance(service, WorkflowClientOptions.newBuilder()
        .setNamespace(namespace())
        .setDataConverter(dataConverter())
        .build());
  }

  public static String namespace() {
    return Env.get("TEMPORAL_NAMESPACE", "default");
  }

  /** Protobuf messages travel as protobuf JSON; with a key set, payloads and failure messages are encrypted. */
  public static DataConverter dataConverter() {
    DataConverter base = DefaultDataConverter.newDefaultInstance();
    String key = Env.get("PAYLOAD_KEY", "");
    if (key.isEmpty()) {
      return base;
    }
    return new CodecDataConverter(base, List.of(new PayloadEncryptionCodec(key, Env.get("PAYLOAD_KEY_ID", "key-1"))), true);
  }

  private static Scope metricsScope() {
    int port = Env.getInt("METRICS_PORT", 9464);
    if (port == 0) {
      return null;
    }
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
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
    } catch (Exception e) {
      log.warn("Metrics endpoint not started: {}", e.toString());
    }
    return new RootScopeBuilder()
        .reporter(new MicrometerClientStatsReporter(registry))
        .reportEvery(com.uber.m3.util.Duration.ofSeconds(10));
  }
}
