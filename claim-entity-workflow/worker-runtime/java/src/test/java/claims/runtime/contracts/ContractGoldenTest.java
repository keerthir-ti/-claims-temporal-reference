package claims.runtime.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;

import claims.contracts.ClaimEvent;
import claims.contracts.ClaimEventType;
import claims.contracts.Decision;
import claims.contracts.PaymentRequest;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackRequest;
import claims.contracts.TrackResult;
import claims.runtime.temporal.TemporalConnection;
import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Reads every golden payload (contracts/golden) through the worker's real data converter, as a Python worker would
 * send it. The Python tests read the same files.
 */
class ContractGoldenTest {
  private static final Path GOLDEN = Path.of("../../contracts/golden");

  private static <T> T read(String file, Class<T> type) throws Exception {
    Payload p = Payload.newBuilder()
        .putMetadata("encoding", ByteString.copyFromUtf8("json/protobuf"))
        .putMetadata("messageType", ByteString.copyFromUtf8(type.getSimpleName().isEmpty() ? "" : "claims." + type.getSimpleName()))
        .setData(ByteString.copyFrom(Files.readAllBytes(GOLDEN.resolve(file))))
        .build();
    return TemporalConnection.dataConverter().fromPayload(p, type, type);
  }

  @Test
  void currentMessagesRead() throws Exception {
    assertEquals(ClaimEventType.CLAIM_UPDATED, read("ClaimEvent.json", ClaimEvent.class).getType());
    assertEquals(1, read("TrackRequest.json", TrackRequest.class).getBufferedEventsCount());
    assertEquals(TrackOutcome.RETRIAGE, read("TrackResult.json", TrackResult.class).getOutcome());
    assertEquals(5_000_000L, read("Decision.json", Decision.class).getActorAuthorityCents());
    assertEquals(2_500_000L, read("PaymentRequest.json", PaymentRequest.class).getAmountCents());
  }

  @Test
  void aNewerWritersMessageStillReads() throws Exception {
    TrackResult r = read("TrackResult.newer-writer.json", TrackResult.class);
    assertEquals("written by a newer contract version", r.getReason());
    // An unknown outcome reads as 0; the parent routes it to a person.
    assertEquals(TrackOutcome.TRACK_OUTCOME_UNSPECIFIED, r.getOutcome());
  }
}
