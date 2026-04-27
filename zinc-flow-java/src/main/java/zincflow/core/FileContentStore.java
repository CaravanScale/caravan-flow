package zincflow.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicLong;

/// Disk-backed {@link ContentStore}. Claims live under
/// {@code baseDir/<2-char-shard>/<claim-id>}, where the shard is the
/// first two characters of the claim id — keeps any one directory from
/// holding millions of files. Writes go to a sibling {@code .part} file
/// and get atomically moved into place so a crash mid-write can't leave
/// a torn claim for a reader to see.
///
/// Mirror of zinc-flow-csharp's FileContentStore, including the
/// {@code claim-<ticks>-<counter>} id scheme so claims sort by creation
/// time. Thread-safe — {@link AtomicLong} drives the counter, and the
/// filesystem provides the rest of the atomicity.
public final class FileContentStore implements ContentStore {

    private final Path baseDir;
    private final AtomicLong counter = new AtomicLong();

    public FileContentStore(Path baseDir) throws IOException {
        if (baseDir == null) throw new IllegalArgumentException("FileContentStore: baseDir must not be null");
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    public Path baseDir() { return baseDir; }

    @Override
    public String store(byte[] data) {
        String id = generateClaimId();
        Path target = claimPath(id);
        Path tmp = target.resolveSibling(target.getFileName() + ".part");
        try {
            Files.createDirectories(target.getParent());
            Files.write(tmp, data);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            throw new RuntimeException("FileContentStore: failed to store claim " + id, ex);
        }
        return id;
    }

    @Override
    public String store(InputStream in) throws IOException {
        String id = generateClaimId();
        Path target = claimPath(id);
        Path tmp = target.resolveSibling(target.getFileName() + ".part");
        Files.createDirectories(target.getParent());
        try (OutputStream out = Files.newOutputStream(tmp)) {
            in.transferTo(out);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        return id;
    }

    @Override
    public byte[] retrieve(String claimId) {
        Path p = claimPath(claimId);
        if (!Files.exists(p)) return new byte[0];
        try { return Files.readAllBytes(p); }
        catch (IOException ex) { throw new RuntimeException("FileContentStore: retrieve failed", ex); }
    }

    @Override
    public InputStream openRead(String claimId) throws IOException {
        return Files.newInputStream(claimPath(claimId));
    }

    @Override
    public long size(String claimId) {
        Path p = claimPath(claimId);
        if (!Files.exists(p)) return -1;
        try { return Files.size(p); }
        catch (IOException ex) { return -1; }
    }

    @Override
    public void delete(String claimId) {
        try { Files.deleteIfExists(claimPath(claimId)); }
        catch (IOException ignored) { /* best-effort */ }
    }

    @Override
    public boolean exists(String claimId) {
        return Files.exists(claimPath(claimId));
    }

    private Path claimPath(String claimId) {
        String shard = claimId.length() >= 2 ? claimId.substring(0, 2) : "00";
        return baseDir.resolve(shard).resolve(claimId);
    }

    private String generateClaimId() {
        return "claim-" + System.currentTimeMillis() + "-" + counter.incrementAndGet();
    }
}
