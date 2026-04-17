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
public final class ReplaceText implements Processor {

    private final Pattern pattern;
    private final String replacement;

    public ReplaceText(String regex, String replacement) {
        if (regex == null) throw new IllegalArgumentException("ReplaceText: regex must not be null");
        this.pattern = Pattern.compile(regex);
        this.replacement = replacement == null ? "" : replacement;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Content content = ff.content();
        if (!(content instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ReplaceText: expected RawContent, got " + content.getClass().getSimpleName(), ff);
        }
        String input = new String(raw.bytes(), StandardCharsets.UTF_8);
        String output = pattern.matcher(input).replaceAll(replacement);
        return ProcessorResult.single(ff.withContent(new RawContent(output.getBytes(StandardCharsets.UTF_8))));
    }
}
