package caravanflow.processors;

import caravanflow.core.ContentResolver;
import caravanflow.core.ContentStore;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.fabric.FlowFileV3;

import java.util.List;

/// Inverse of {@link PackageFlowFileV3}: treat the FlowFile's content
/// as V3-framed bytes (which may hold N FlowFiles concatenated) and
/// emit each unpacked FlowFile with its original attributes restored.
///
/// On non-V3 input (no magic header) emits a Failure so the caller can
/// route to an error handler rather than silently discarding the blob.
public final class UnpackageFlowFileV3 implements Processor {

    private final ContentStore store;

    public UnpackageFlowFileV3() { this(null); }

    public UnpackageFlowFileV3(ContentStore store) { this.store = store; }

    @Override
    public ProcessorResult process(FlowFile ff) {
        ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
        if (!resolved.ok()) {
            return ProcessorResult.failure("UnpackageFlowFileV3: " + resolved.error(), ff);
        }
        byte[] data = resolved.bytes();
        if (data.length < FlowFileV3.MAGIC_LEN) {
            return ProcessorResult.failure(
                    "UnpackageFlowFileV3: payload too small for V3 magic", ff);
        }
        for (int i = 0; i < FlowFileV3.MAGIC_LEN; i++) {
            if (data[i] != FlowFileV3.MAGIC[i]) {
                return ProcessorResult.failure(
                        "UnpackageFlowFileV3: missing NiFiFF3 magic — not a V3-framed FlowFile", ff);
            }
        }
        List<FlowFile> unpacked = FlowFileV3.unpackAll(data);
        if (unpacked.isEmpty()) {
            return ProcessorResult.failure(
                    "UnpackageFlowFileV3: V3 stream contained no FlowFiles", ff);
        }
        if (unpacked.size() == 1) {
            return ProcessorResult.single(unpacked.get(0));
        }
        return ProcessorResult.multiple(unpacked);
    }
}
