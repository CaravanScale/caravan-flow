package zincflow.processors;

import zincflow.core.ContentResolver;
import zincflow.core.ContentStore;
import zincflow.core.FlowFile;
import zincflow.core.FlowFileAttributes;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;
import zincflow.fabric.FlowFileV3;

/// Wrap a FlowFile's attributes + content into a single NiFi FlowFile V3
/// binary blob. The output FlowFile carries that blob as its raw
/// content; downstream sinks (PutFile / PutHTTP / PutStdout) write it
/// verbatim, and another V3-aware reader can round-trip the original
/// attributes.
///
/// Turns V3 framing into a pipeline step rather than a sink-only
/// concern — mirror of zinc-flow-csharp's PackageFlowFileV3.
public final class PackageFlowFileV3 implements Processor {

    private final ContentStore store;

    public PackageFlowFileV3() { this(null); }

    public PackageFlowFileV3(ContentStore store) { this.store = store; }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (ff.content() instanceof RecordContent) {
            return ProcessorResult.failure(
                    "PackageFlowFileV3: RecordContent not supported — serialize to raw first", ff);
        }
        ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
        if (!resolved.ok()) {
            return ProcessorResult.failure("PackageFlowFileV3: " + resolved.error(), ff);
        }
        byte[] packed = FlowFileV3.pack(ff, resolved.bytes());
        FlowFile out = ff.withContent(new RawContent(packed))
                .withAttribute(FlowFileAttributes.HTTP_CONTENT_TYPE, "application/flowfile-v3")
                .withAttribute("v3.packaged", "true");
        return ProcessorResult.single(out);
    }
}
