package com.example.claims.temporal;

import com.example.claims.temporal.client.ContractJson;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackOutcome;
import com.example.claims.contracts.TrackResult;
import io.temporal.common.converter.DataConverter;
import io.temporal.api.common.v1.Payloads;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Same golden files as python/tests/test_contracts.py, decoded through the real Temporal DataConverter. */
class ContractGoldenTest {
  private static final Path GOLDEN = Path.of(System.getProperty("contracts.dir", "../contracts"), "golden");
  private static final ObjectMapper JSON = ContractJson.MAPPER;
  private static final DataConverter CONVERTER = ContractJson.dataConverter();

  @TestFactory
  Stream<DynamicTest> goldenRoundTrip() throws Exception {
    List<Path> files;
    try (Stream<Path> s = Files.list(GOLDEN)) {
      files = s.filter(p -> p.toString().endsWith(".json")).sorted().toList();
    }
    return files.stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> {
      String name = p.getFileName().toString().replace(".json", "");
      Class<?> type = Class.forName("com.example.claims.contracts." + name);
      JsonNode original = JSON.readTree(p.toFile());
      Object decoded = decode(original, type);
      JsonNode encoded = JSON.readTree(CONVERTER.toPayload(decoded).get().getData().toByteArray());
      assertEquals(original, encoded);
    }));
  }

  @Test
  void forwardCompatibleRead() throws Exception {
    JsonNode raw = JSON.readTree(GOLDEN.resolve("forward-compat/TrackResult.json").toFile());
    TrackResult r = decode(raw, TrackResult.class);
    assertEquals(TrackOutcome.UNKNOWN, r.getOutcome());
    assertEquals(Track.STP, r.getTrack());
  }

  private static <T> T decode(JsonNode json, Class<T> type) throws Exception {
    ObjectNode copy = ((ObjectNode) json).deepCopy();
    Payloads payloads = CONVERTER.toPayloads(copy).get();
    return CONVERTER.fromPayloads(0, Optional.of(payloads), type, type);
  }
}
