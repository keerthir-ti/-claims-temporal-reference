package claims.runtime.temporal;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.temporal.api.common.v1.Payload;
import io.temporal.payload.codec.PayloadCodec;
import io.temporal.payload.codec.PayloadCodecException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-256-GCM payload encryption, byte-compatible with the Python codec in workers/ai-worker-common.
 * Data = 12-byte nonce + ciphertext + 16-byte tag; the plaintext is the serialized original Payload.
 * Encodes with the current key; decodes with the current or any previous key, so a key rotation never makes
 * histories of running or retained workflows unreadable.
 */
public final class PayloadEncryptionCodec implements PayloadCodec {
  static final ByteString ENCODING = ByteString.copyFrom("binary/encrypted", StandardCharsets.UTF_8);
  static final String KEY_ID = "encryption-key-id";
  private static final SecureRandom RANDOM = new SecureRandom();

  private final SecretKeySpec key;
  private final String keyId;
  private final Map<String, SecretKeySpec> keys = new HashMap<>();

  /** {@code previousKeys}: "id=base64,id=base64" of keys retired from encoding but still needed to decode. */
  public PayloadEncryptionCodec(String base64Key, String keyId, String previousKeys) {
    this.key = aesKey(base64Key);
    this.keyId = keyId;
    keys.put(keyId, key);
    for (String entry : previousKeys.split(",")) {
      if (!entry.isBlank()) {
        String[] idAndKey = entry.trim().split("=", 2);
        keys.put(idAndKey[0], aesKey(idAndKey[1]));
      }
    }
  }

  private static SecretKeySpec aesKey(String base64Key) {
    byte[] raw = Base64.getDecoder().decode(base64Key);
    if (raw.length != 32) {
      throw new IllegalArgumentException("Payload keys must be 32 bytes, base64 encoded");
    }
    return new SecretKeySpec(raw, "AES");
  }

  @Override
  public List<Payload> encode(List<Payload> payloads) {
    List<Payload> out = new ArrayList<>(payloads.size());
    for (Payload p : payloads) {
      try {
        byte[] nonce = new byte[12];
        RANDOM.nextBytes(nonce);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        byte[] sealed = cipher.doFinal(p.toByteArray());
        out.add(Payload.newBuilder()
            .putMetadata("encoding", ENCODING)
            .putMetadata(KEY_ID, ByteString.copyFrom(keyId, StandardCharsets.UTF_8))
            .setData(ByteString.copyFrom(nonce).concat(ByteString.copyFrom(sealed)))
            .build());
      } catch (GeneralSecurityException e) {
        throw new PayloadCodecException(e);
      }
    }
    return out;
  }

  @Override
  public List<Payload> decode(List<Payload> payloads) {
    List<Payload> out = new ArrayList<>(payloads.size());
    for (Payload p : payloads) {
      if (!ENCODING.equals(p.getMetadataOrDefault("encoding", ByteString.EMPTY))) {
        out.add(p);
        continue;
      }
      String id = p.getMetadataOrDefault(KEY_ID, ByteString.EMPTY).toStringUtf8();
      SecretKeySpec decodeKey = keys.get(id);
      if (decodeKey == null) {
        throw new PayloadCodecException("Unknown payload key id " + id);
      }
      try {
        byte[] data = p.getData().toByteArray();
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, decodeKey, new GCMParameterSpec(128, data, 0, 12));
        out.add(Payload.parseFrom(cipher.doFinal(data, 12, data.length - 12)));
      } catch (GeneralSecurityException | InvalidProtocolBufferException e) {
        throw new PayloadCodecException(e);
      }
    }
    return out;
  }
}
