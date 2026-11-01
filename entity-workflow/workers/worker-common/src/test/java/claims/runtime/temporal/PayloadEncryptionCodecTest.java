package claims.runtime.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.payload.codec.PayloadCodecException;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadEncryptionCodecTest {
  private static final String KEY_1 = Base64.getEncoder().encodeToString(new byte[32]);
  private static final String KEY_2 = Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef".getBytes());
  private static final Payload PLAIN = Payload.newBuilder()
      .putMetadata("encoding", ByteString.copyFromUtf8("json/protobuf"))
      .setData(ByteString.copyFromUtf8("{\"claimId\":\"C1\"}")).build();

  @Test
  void encryptsAndDecrypts() {
    PayloadEncryptionCodec codec = new PayloadEncryptionCodec(KEY_1, "key-1", "");
    Payload sealed = codec.encode(List.of(PLAIN)).get(0);
    assertNotEquals(PLAIN.getData(), sealed.getData());
    assertEquals(PLAIN, codec.decode(List.of(sealed)).get(0));
  }

  @Test
  void afterRotationOldHistoriesStillDecode() {
    Payload sealedWithOldKey = new PayloadEncryptionCodec(KEY_1, "key-1", "").encode(List.of(PLAIN)).get(0);
    PayloadEncryptionCodec rotated = new PayloadEncryptionCodec(KEY_2, "key-2", "key-1=" + KEY_1);
    assertEquals(PLAIN, rotated.decode(List.of(sealedWithOldKey)).get(0));
    assertEquals("key-2", rotated.encode(List.of(PLAIN)).get(0).getMetadataOrThrow("encryption-key-id").toStringUtf8());
  }

  @Test
  void unknownKeyIsRefused() {
    Payload sealed = new PayloadEncryptionCodec(KEY_1, "key-1", "").encode(List.of(PLAIN)).get(0);
    assertThrows(PayloadCodecException.class, () -> new PayloadEncryptionCodec(KEY_2, "key-2", "").decode(List.of(sealed)));
  }
}
