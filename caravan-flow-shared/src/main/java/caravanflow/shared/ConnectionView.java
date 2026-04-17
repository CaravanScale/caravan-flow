package caravanflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/// View of the full connection map under {@code GET /api/flow}'s
/// {@code connections: {...}} field. Inner shape:
/// {@code fromProcessor → relationship → [targetProcessors]}.
/// This record is a thin wrapper that lets callers keep the map
/// typed without repeating the parametric spelling everywhere.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectionView(
        Map<String, Map<String, List<String>>> map) {

    public static ConnectionView empty() {
        return new ConnectionView(Map.of());
    }
}
