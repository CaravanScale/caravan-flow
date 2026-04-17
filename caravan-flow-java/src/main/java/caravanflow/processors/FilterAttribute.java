package caravanflow.processors;

import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/// Keeps or removes a named set of attributes from the FlowFile, always
/// passing the FlowFile through. Mirrors caravan-flow-csharp's
/// {@code FilterAttribute} (StdLib/Processors.cs:62-90).
///
/// Config:
///   mode       — "remove" (default) or "keep"
///   attributes — semicolon-separated attribute names
public final class FilterAttribute implements Processor {

    private final boolean removeMode;
    private final Set<String> attributeSet;

    public FilterAttribute(String mode, String attributes) {
        this.removeMode = !"keep".equalsIgnoreCase(mode);
        this.attributeSet = attributes == null || attributes.isBlank()
                ? Set.of()
                : Arrays.stream(attributes.split(";"))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Map<String, String> filtered = new LinkedHashMap<>();
        for (var entry : ff.attributes().entrySet()) {
            boolean listed = attributeSet.contains(entry.getKey());
            if (removeMode ? !listed : listed) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return ProcessorResult.single(new FlowFile(
                ff.id(), filtered, ff.content(), ff.timestampMillis(), ff.hopCount()));
    }
}
