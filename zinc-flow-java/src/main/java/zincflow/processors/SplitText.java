package zincflow.processors;

import zincflow.core.Content;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/// Splits the FlowFile's text payload into multiple FlowFiles — one per
/// split segment. Each resulting FlowFile inherits the parent's
/// attributes plus {@code split.index} and {@code split.parent}. The
/// executor fans out the returned Multiple on "success."
public final class SplitText implements Processor {

    private final String delimiter;
    private final boolean regex;

    public SplitText(String delimiter) { this(delimiter, false); }

    public SplitText(String delimiter, boolean regex) {
        if (delimiter == null || delimiter.isEmpty()) {
            throw new IllegalArgumentException("SplitText: delimiter must not be blank");
        }
        this.delimiter = delimiter;
        this.regex = regex;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Content content = ff.content();
        if (!(content instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "SplitText: expected RawContent, got " + content.getClass().getSimpleName(), ff);
        }
        String input = new String(raw.bytes(), StandardCharsets.UTF_8);
        String[] pieces = regex ? input.split(delimiter) : input.split(java.util.regex.Pattern.quote(delimiter));
        List<FlowFile> out = new ArrayList<>(pieces.length);
        for (int i = 0; i < pieces.length; i++) {
            FlowFile piece = ff
                    .withContent(new RawContent(pieces[i].getBytes(StandardCharsets.UTF_8)))
                    .withAttribute("split.index", String.valueOf(i))
                    .withAttribute("split.parent", ff.stringId());
            out.add(piece);
        }
        return ProcessorResult.multiple(out);
    }
}
