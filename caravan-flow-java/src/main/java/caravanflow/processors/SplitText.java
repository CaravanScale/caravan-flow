package caravanflow.processors;

import caravanflow.core.Content;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/// Splits the FlowFile's text payload into multiple FlowFiles — one per
/// non-empty split segment. Mirrors caravan-flow-csharp's {@code SplitText}
/// (StdLib/TextProcessors.cs:100-160):
///   delimiter   — always treated as a regex
///   headerLines — number of leading lines to prepend to every chunk
///                 (for CSV-style header replication); 0 disables
///
/// Emitted FlowFiles carry {@code split.index} and {@code split.count}
/// attributes; the original attributes are NOT inherited (matches C#).
/// When the delimiter doesn't match, the input FlowFile passes through
/// unchanged.
public final class SplitText implements Processor {

    private final Pattern delimiter;
    private final int headerLines;

    public SplitText(String delimiter) { this(delimiter, 0); }

    public SplitText(String delimiter, int headerLines) {
        if (delimiter == null || delimiter.isEmpty()) {
            throw new IllegalArgumentException("SplitText: delimiter must not be blank");
        }
        this.delimiter = Pattern.compile(delimiter);
        this.headerLines = Math.max(0, headerLines);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Content content = ff.content();
        if (!(content instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "SplitText: expected RawContent, got " + content.getClass().getSimpleName(), ff);
        }
        String text = new String(raw.bytes(), StandardCharsets.UTF_8);
        String[] parts = delimiter.split(text, -1);

        if (parts.length <= 1) {
            return ProcessorResult.single(ff);
        }

        String header = "";
        if (headerLines > 0) {
            String[] lines = text.split("\n", -1);
            if (lines.length > headerLines) {
                StringBuilder hb = new StringBuilder();
                for (int i = 0; i < headerLines; i++) hb.append(lines[i]).append('\n');
                header = hb.toString();
                StringBuilder rem = new StringBuilder();
                for (int i = headerLines; i < lines.length; i++) {
                    rem.append(lines[i]);
                    if (i < lines.length - 1) rem.append('\n');
                }
                parts = delimiter.split(rem.toString(), -1);
            }
        }

        List<FlowFile> out = new ArrayList<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].isBlank()) continue;
            String chunk = header + parts[i];
            FlowFile piece = FlowFile.create(
                    chunk.getBytes(StandardCharsets.UTF_8),
                    java.util.Map.of(
                            "split.index", String.valueOf(i),
                            "split.count", String.valueOf(parts.length)));
            out.add(piece);
        }

        if (out.isEmpty()) {
            return ProcessorResult.single(ff);
        }
        return ProcessorResult.multiple(out);
    }
}
