package zincflow.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.Content;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// Writes the FlowFile payload to a file under {@code directory}. The
/// file name defaults to {@code ff-<id>} but the {@code filename}
/// attribute overrides if present. Returns a Failure on I/O errors so
/// operators can wire failure-handling processors.
public final class PutFile implements Processor {

    private static final Logger log = LoggerFactory.getLogger(PutFile.class);

    private final Path directory;
    private final boolean append;

    public PutFile(String directory) { this(directory, false); }

    public PutFile(String directory, boolean append) {
        if (directory == null || directory.isEmpty()) {
            throw new IllegalArgumentException("PutFile: directory must not be blank");
        }
        this.directory = Path.of(directory);
        this.append = append;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        String name = ff.attributes().getOrDefault("filename", ff.stringId());
        Path target = directory.resolve(name);
        try {
            Files.createDirectories(directory);
            if (ff.content() instanceof RawContent raw) {
                if (append) {
                    Files.write(target, raw.bytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } else {
                    Files.write(target, raw.bytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                }
            } else {
                Content c = ff.content();
                log.warn("PutFile can only handle RawContent in Phase 3 — got {} for {}",
                        c.getClass().getSimpleName(), ff.stringId());
                return ProcessorResult.failure("PutFile: unsupported Content type " + c.getClass().getSimpleName(), ff);
            }
            return ProcessorResult.single(ff.withAttribute("putfile.path", target.toAbsolutePath().toString()));
        } catch (IOException ex) {
            log.error("PutFile: write failed for {}: {}", target, ex.toString());
            return ProcessorResult.failure(ex.getMessage(), ff);
        }
    }
}
