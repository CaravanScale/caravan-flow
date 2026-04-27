package zincflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/// One source entry under {@code GET /api/flow}'s
/// {@code sources: [...]} array.
@JsonIgnoreProperties(ignoreUnknown = true)
public record SourceView(String name, String type, boolean running) {
}
