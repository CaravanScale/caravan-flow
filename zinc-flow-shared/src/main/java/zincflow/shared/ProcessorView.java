package zincflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/// One processor as it appears under {@code GET /api/flow}'s
/// {@code processors: [...]} array. {@code stats} is a flat map of
/// counter-name → value; {@code connections} is
/// relationship → list-of-target-processors.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProcessorView(
        String name,
        String type,
        String state,
        Map<String, Long> stats,
        Map<String, List<String>> connections) {
}
