package zincflow.processors;

import zincflow.core.Content;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;

import java.nio.charset.StandardCharsets;

/// Writes the FlowFile's payload to {@code System.out} and terminates
/// this branch with Dropped. Useful as a sink in demo pipelines and
/// smoke tests.
public final class PutStdout implements Processor {

    private final String prefix;

    public PutStdout() { this(""); }

    public PutStdout(String prefix) {
        this.prefix = prefix == null ? "" : prefix;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Content content = ff.content();
        if (content instanceof RawContent raw) {
            System.out.println(prefix + new String(raw.bytes(), StandardCharsets.UTF_8));
        } else {
            System.out.println(prefix + "<non-raw content: size=" + content.size() + ">");
        }
        return ProcessorResult.dropped();
    }
}
