package zincflow.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ClaimContent;
import zincflow.core.ContentResolver;
import zincflow.core.ContentStore;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.fabric.FlowFileV3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// Writes the FlowFile payload to a file under {@code directory}. The
/// file name defaults to the FlowFile's string id; the {@code filename}
/// attribute overrides when present.
///
/// {@code format=v3} packs the attributes + content into a NiFi
/// FlowFile V3 blob before writing — lossless round-trip through
/// another V3-aware reader. Claim-backed content resolves through the
/// supplied {@link ContentStore}.
public final class PutFile implements Processor {

    private static final Logger log = LoggerFactory.getLogger(PutFile.class);

    private final Path directory;
    private final boolean append;
    private final boolean v3;
    private final ContentStore store;

    public PutFile(String directory) { this(directory, false); }

    public PutFile(String directory, boolean append) { this(directory, append, "raw", null); }

    public PutFile(String directory, boolean append, String format, ContentStore store) {
        if (directory == null || directory.isEmpty()) {
            throw new IllegalArgumentException("PutFile: directory must not be blank");
        }
        this.directory = Path.of(directory);
        this.append = append;
        this.v3 = "v3".equalsIgnoreCase(format);
        this.store = store;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        String name = ff.attributes().getOrDefault("filename", ff.stringId());
        Path target = directory.resolve(name);
        try {
            Files.createDirectories(directory);

            // V3 framing inlines the attribute header + byte body, so we
            // always resolve to a single buffer for that path. The
            // non-V3 path streams ClaimContent directly so multi-GB
            // payloads never get loaded into the heap.
            if (v3) {
                ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
                if (!resolved.ok()) return fail(ff, resolved.error());
                byte[] packed = FlowFileV3.pack(ff, resolved.bytes());
                writeBytes(target, packed);
            } else if (ff.content() instanceof ClaimContent claim && store != null) {
                try (InputStream in = store.openRead(claim.claimId());
                     OutputStream out = Files.newOutputStream(target, openOptions())) {
                    in.transferTo(out);
                }
            } else if (ff.content() instanceof RawContent raw) {
                writeBytes(target, raw.bytes());
            } else {
                ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
                if (!resolved.ok()) return fail(ff, resolved.error());
                writeBytes(target, resolved.bytes());
            }
            return ProcessorResult.single(ff.withAttribute("putfile.path", target.toAbsolutePath().toString()));
        } catch (IOException ex) {
            log.error("PutFile: write failed for {}: {}", target, ex.toString());
            return ProcessorResult.failure(ex.getMessage(), ff);
        }
    }

    private ProcessorResult fail(FlowFile ff, String reason) {
        log.error("PutFile: {}", reason);
        return ProcessorResult.failure("PutFile: " + reason, ff);
    }

    private void writeBytes(Path target, byte[] bytes) throws IOException {
        Files.write(target, bytes, openOptions());
    }

    private StandardOpenOption[] openOptions() {
        return append
                ? new StandardOpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.APPEND }
                : new StandardOpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };
    }
}
