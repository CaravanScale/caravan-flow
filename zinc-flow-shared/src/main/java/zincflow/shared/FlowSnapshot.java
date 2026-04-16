package zincflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/// Full graph snapshot returned by {@code GET /api/flow}. Used by the
/// UI's {@code /flow} view to render processors and connections.
/// {@code stats} is the global pipeline counter snapshot (processed,
/// dropped, failed — names match {@code Stats.snapshot}).
@JsonIgnoreProperties(ignoreUnknown = true)
public record FlowSnapshot(
        List<String> entryPoints,
        List<ProcessorView> processors,
        Map<String, Map<String, List<String>>> connections,
        List<ProviderView> providers,
        List<SourceView> sources,
        Map<String, Object> stats) {

    public static FlowSnapshot empty() {
        return new FlowSnapshot(List.of(), List.of(), Map.of(), List.of(), List.of(), Map.of());
    }
}
