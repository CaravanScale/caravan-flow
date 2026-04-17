package caravanflow.processors;

import caravanflow.core.ContentResolver;
import caravanflow.core.ContentStore;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.fabric.FlowFileV3;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/// Writes the FlowFile's payload to {@code System.out} and terminates
/// the branch with Dropped. {@code format} picks the rendering:
/// <ul>
///   <li>{@code raw} (default) — UTF-8 text</li>
///   <li>{@code hex} — first 128 bytes hex-encoded</li>
///   <li>{@code v3} — pack + hex-dump the V3 framing (useful for
///       debugging V3 interop)</li>
/// </ul>
/// Claim-backed content resolves through the supplied
/// {@link ContentStore}.
public final class PutStdout implements Processor {

    private final String prefix;
    private final Format format;
    private final ContentStore store;

    public PutStdout() { this("", "raw", null); }

    public PutStdout(String prefix) { this(prefix, "raw", null); }

    public PutStdout(String prefix, String format, ContentStore store) {
        this.prefix = prefix == null ? "" : prefix;
        this.format = Format.parse(format);
        this.store = store;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
        if (!resolved.ok()) {
            return ProcessorResult.failure("PutStdout: " + resolved.error(), ff);
        }
        byte[] bytes = resolved.bytes();
        switch (format) {
            case RAW -> System.out.println(prefix + new String(bytes, StandardCharsets.UTF_8));
            case HEX -> System.out.println(prefix + "(" + bytes.length + " bytes) "
                    + HexFormat.of().formatHex(bytes, 0, Math.min(bytes.length, 128)));
            case V3 -> {
                byte[] packed = FlowFileV3.pack(ff, bytes);
                System.out.println(prefix + "v3 (" + packed.length + " bytes) "
                        + HexFormat.of().formatHex(packed, 0, Math.min(packed.length, 128)));
            }
        }
        return ProcessorResult.dropped();
    }

    private enum Format {
        RAW, HEX, V3;
        static Format parse(String s) {
            if (s == null || s.isBlank()) return RAW;
            return switch (s.toLowerCase()) {
                case "raw", "text" -> RAW;
                case "hex"          -> HEX;
                case "v3"           -> V3;
                default -> throw new IllegalArgumentException(
                        "PutStdout: format must be raw/hex/v3, got '" + s + "'");
            };
        }
    }
}
