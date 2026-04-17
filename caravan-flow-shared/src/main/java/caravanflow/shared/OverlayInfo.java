package caravanflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/// Response shape of {@code GET /api/overlays}. The worker's
/// {@code ConfigLoader} composes a stack of YAML files (base +
/// role-specific overlays + secrets), and this DTO exposes both the
/// composition and the merged result so the UI can render a
/// read-only view of what's been applied.
///
/// {@code effective} and {@code provenance} are intentionally typed
/// as nested maps — their shape mirrors whatever the user's YAML
/// contains, which ranges from flat key-value to arbitrarily nested
/// config trees. {@code provenance} keys are dotted paths ("flow.processors.ingress")
/// and values are the layer role where that path last won.
@JsonIgnoreProperties(ignoreUnknown = true)
public record OverlayInfo(
        String base,
        List<Layer> layers,
        Map<String, Object> effective,
        Map<String, String> provenance) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Layer(String role, String path, boolean present, int size) {}
}
