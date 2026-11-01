package claims.lifecycle.archive;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Mock of the S3 archive: a local folder (a Docker volume). Replace with an S3 implementation. */
public class LocalArchiveStore implements ArchiveStore {
  private final Path root;

  public LocalArchiveStore(Path root) {
    this.root = root;
  }

  @Override
  public void put(String key, byte[] data) {
    try {
      Path target = root.resolve(key);
      Files.createDirectories(target.getParent());
      Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
      Files.write(tmp, data);
      Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public byte[] get(String key) {
    try {
      return Files.readAllBytes(root.resolve(key));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public boolean exists(String key) {
    return Files.exists(root.resolve(key));
  }

  @Override
  public String historyKey(String workflowId, String runId) {
    return "histories/" + workflowId.replace(':', '_') + "/" + runId + ".json";
  }
}
