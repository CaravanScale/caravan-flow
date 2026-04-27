package zincflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/// One processor as it appears under {@code GET /api/flow}'s
/// {@code processors: [...]} array. {@code stats} is a flat map of
/// counter-name → value; {@code connections} is
/// relationship → list-of-target-processors; {@code config} is the
/// processor's k/v configuration as captured at registration.
///
/// {@code config} may be null when consumed from an older worker
/// that doesn't emit the field — callers should treat null as
/// "empty map".
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProcessorView(
        String name,
        String type,
        String state,
        Map<String, String> config,
        Map<String, Long> stats,
        Map<String, List<String>> connections) {
}
