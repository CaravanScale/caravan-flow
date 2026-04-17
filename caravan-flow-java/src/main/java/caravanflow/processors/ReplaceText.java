package caravanflow.processors;

import caravanflow.core.Content;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/// Regex search-and-replace on the FlowFile's text payload. The match
/// pattern is treated as a Java regex; the replacement supports back
/// references like {@code $1} per {@link java.util.regex.Matcher#replaceAll}.
///
/// Config mirrors caravan-flow-csharp's {@code ReplaceText}
/// (StdLib/TextProcessors.cs:11-40):
///   pattern     — required regex
///   replacement — default ""
///   mode        — "all" (default) or "first"
public final class ReplaceText implements Processor {

    private final Pattern pattern;
    private final String replacement;
    private final boolean firstOnly;

    public ReplaceText(String pattern, String replacement) {
        this(pattern, replacement, "all");
    }

    public ReplaceText(String pattern, String replacement, String mode) {
        if (pattern == null) throw new IllegalArgumentException("ReplaceText: pattern must not be null");
        this.pattern = Pattern.compile(pattern);
        this.replacement = replacement == null ? "" : replacement;
        this.firstOnly = "first".equalsIgnoreCase(mode);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Content content = ff.content();
        if (!(content instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ReplaceText: expected RawContent, got " + content.getClass().getSimpleName(), ff);
        }
        String input = new String(raw.bytes(), StandardCharsets.UTF_8);
        var matcher = pattern.matcher(input);
        String output = firstOnly ? matcher.replaceFirst(replacement) : matcher.replaceAll(replacement);
        return ProcessorResult.single(ff.withContent(new RawContent(output.getBytes(StandardCharsets.UTF_8))));
    }
}
