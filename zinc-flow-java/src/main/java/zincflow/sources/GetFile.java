package zincflow.sources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.FlowFile;
import zincflow.core.FlowFileAttributes;
import zincflow.core.PollingSource;
import zincflow.core.Source;
import zincflow.core.SourcePlugin;
import zincflow.fabric.FlowFileV3;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Poll a directory for files and stream them into the pipeline. Files
/// consumed by the pipeline are moved to a {@code .processed/}
/// subdirectory so the next poll doesn't re-emit them.
///
/// <h2>V3 unpacking</h2>
/// When {@code unpackV3} is true (default) and a scanned file begins
/// with the {@code NiFiFF3} magic, the source unpacks every FlowFile
/// in the blob and emits them individually. Attributes from the packed
/// FlowFiles are preserved; the source layers on top:
/// {@code filename}, {@code path}, {@code source},
/// {@code v3.frame.index}, {@code v3.frame.count}.
/// Regular files get {@code filename}, {@code path}, {@code size}, {@code source}.
///
/// Config (under {@code sources.file}):
/// <pre>
/// sources:
///   file:
///     inputDir: /var/spool/zincflow
///     pattern: "*"
///     pollIntervalMs: 1000
///     unpackV3: true
/// </pre>
///
/// Mirrors zinc-flow-csharp's GetFile.
public final class GetFile extends PollingSource {

    private static final Logger log = LoggerFactory.getLogger(GetFile.class);

    public static final String NAME = "file";
    public static final String TYPE = "GetFile";
    public static final String PROCESSED_DIR = ".processed";

    private final Path inputDir;
    private final String pattern;
    private final boolean unpackV3;
    private final Path processedDir;
    // filename-per-FlowFile so onIngested can move the right source
    // file. We can't key on attrs because V3 unpacking emits N FlowFiles
    // per file and we only move the underlying file once all N land.
    private final Map<Long, Path> pendingMoves = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<Path, Integer> outstandingPerFile = new java.util.concurrent.ConcurrentHashMap<>();

    public GetFile(String name, Path inputDir, String pattern,
                   long pollIntervalMillis, boolean unpackV3) {
        super(name, pollIntervalMillis);
        if (inputDir == null) throw new IllegalArgumentException("inputDir must not be null");
        this.inputDir = inputDir;
        this.pattern = (pattern == null || pattern.isEmpty()) ? "*" : pattern;
        this.unpackV3 = unpackV3;
        this.processedDir = inputDir.resolve(PROCESSED_DIR);
    }

    @Override public String sourceType() { return TYPE; }
    public Path inputDir() { return inputDir; }
    public Path processedDir() { return processedDir; }

    @Override
    protected List<FlowFile> poll() {
        ensureDirs();
        List<FlowFile> out = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(inputDir, pattern)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) continue;
                List<FlowFile> emitted = emitFor(entry);
                if (!emitted.isEmpty()) {
                    outstandingPerFile.put(entry, emitted.size());
                    for (FlowFile ff : emitted) pendingMoves.put(ff.id(), entry);
                    out.addAll(emitted);
                }
            }
        } catch (IOException ex) {
            log.warn("GetFile {}: directory scan failed — {}", name(), ex.toString());
        }
        return out;
    }

    private List<FlowFile> emitFor(Path file) {
        byte[] bytes;
        try { bytes = Files.readAllBytes(file); }
        catch (IOException ex) {
            // File might have been removed or partially written between
            // the listing and the read — skip this round, the next poll
            // picks it up.
            log.debug("GetFile {}: could not read {} ({}) — skipping", name(), file, ex.toString());
            return List.of();
        }

        if (unpackV3 && looksLikeV3(bytes)) {
            List<FlowFile> frames = FlowFileV3.unpackAll(bytes);
            if (!frames.isEmpty()) {
                List<FlowFile> out = new ArrayList<>(frames.size());
                for (int i = 0; i < frames.size(); i++) {
                    out.add(addAttrs(frames.get(i), file, bytes.length, true, i, frames.size()));
                }
                return out;
            }
            // Well-formed magic but no frames decoded — fall through and
            // treat the file as raw. Losing malformed V3 silently would
            // hide bugs; treating it as raw surfaces the bytes for ops.
        }

        return List.of(addAttrs(FlowFile.create(bytes, Map.of()), file, bytes.length, false, 0, 1));
    }

    private FlowFile addAttrs(FlowFile base, Path file, long rawSize,
                              boolean v3, int frameIndex, int frameCount) {
        Map<String, String> attrs = new LinkedHashMap<>(base.attributes());
        attrs.put(FlowFileAttributes.FILENAME, file.getFileName().toString());
        attrs.put(FlowFileAttributes.PATH, file.toAbsolutePath().toString());
        attrs.put(FlowFileAttributes.SOURCE, name());
        if (v3) {
            attrs.put(FlowFileAttributes.V3_FRAME_INDEX, Integer.toString(frameIndex));
            attrs.put(FlowFileAttributes.V3_FRAME_COUNT, Integer.toString(frameCount));
        } else {
            attrs.put(FlowFileAttributes.SIZE, Long.toString(rawSize));
        }
        return new FlowFile(base.id(), attrs, base.content(), base.timestampMillis(), base.hopCount());
    }

    private static boolean looksLikeV3(byte[] data) {
        if (data.length < FlowFileV3.MAGIC_LEN) return false;
        for (int i = 0; i < FlowFileV3.MAGIC_LEN; i++) {
            if (data[i] != FlowFileV3.MAGIC[i]) return false;
        }
        return true;
    }

    @Override
    protected void onIngested(FlowFile ff) {
        Path file = pendingMoves.remove(ff.id());
        if (file == null) return;
        Integer remaining = outstandingPerFile.computeIfPresent(file, (k, v) -> v - 1);
        if (remaining != null && remaining <= 0) {
            outstandingPerFile.remove(file);
            moveToProcessed(file);
        }
    }

    @Override
    protected void onRejected(FlowFile ff) {
        // Drop bookkeeping without moving the file — it stays in
        // inputDir for the next poll to retry.
        Path file = pendingMoves.remove(ff.id());
        if (file != null) outstandingPerFile.remove(file);
        super.onRejected(ff);
    }

    private void moveToProcessed(Path file) {
        try {
            Files.createDirectories(processedDir);
            Path target = processedDir.resolve(file.getFileName());
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            // Non-atomic fallback — some filesystems (e.g. across mount
            // boundaries) don't support ATOMIC_MOVE. Best-effort copy;
            // a failure here means the file will re-ingest on the next
            // poll, which is safer than dropping it.
            try {
                Files.createDirectories(processedDir);
                Files.move(file, processedDir.resolve(file.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex2) {
                log.warn("GetFile {}: failed to move {} to {} — {}", name(), file, processedDir, ex2.toString());
            }
        }
    }

    private void ensureDirs() {
        try { Files.createDirectories(inputDir); }
        catch (IOException ex) { /* tolerate — poll will surface issues */ }
    }

    /// SPI entry for ServiceLoader discovery.
    public static final class Plugin implements SourcePlugin {
        @Override public String sourceType() { return TYPE; }
        @Override public String description() { return "Polls a directory; emits one FlowFile per file (V3 bundles are unpacked)."; }
        @Override public List<String> configKeys() {
            return List.of("inputDir", "pattern", "pollIntervalMs", "unpackV3");
        }
        @Override public Source create(String name, Map<String, Object> config) {
            String inputDir = str(config.get("inputDir"));
            if (inputDir.isEmpty()) return null; // disabled when inputDir is absent
            return new GetFile(name,
                    Path.of(inputDir),
                    str(config.getOrDefault("pattern", "*")),
                    longOr(config.get("pollIntervalMs"), 1000),
                    boolOr(config.get("unpackV3"), true));
        }

        private static String str(Object o) { return o == null ? "" : String.valueOf(o); }
        private static long longOr(Object o, long fallback) {
            if (o == null) return fallback;
            if (o instanceof Number n) return n.longValue();
            try { return Long.parseLong(o.toString().trim()); }
            catch (NumberFormatException ex) { return fallback; }
        }
        private static boolean boolOr(Object o, boolean fallback) {
            if (o == null) return fallback;
            if (o instanceof Boolean b) return b;
            return "true".equalsIgnoreCase(o.toString().trim());
        }
    }
}
