package zincflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/// One provider entry under {@code GET /api/flow}'s
/// {@code providers: [...]} array.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProviderView(String name, String type, String state) {
}
