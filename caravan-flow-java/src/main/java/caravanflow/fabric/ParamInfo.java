package caravanflow.fabric;

import java.util.List;

/// Typed description of a single processor config parameter. Drives the UI
/// form so visual programming can show proper typed inputs instead of raw
/// key/value text boxes.
///
/// <h2>Default semantics</h2>
/// <ul>
///   <li>{@code defaultValue == null} → "no default, leave blank"</li>
///   <li>{@code defaultValue == ""} → "default is the empty string"</li>
/// </ul>
/// The UI preserves this distinction when seeding form state.
public record ParamInfo(
        String name,
        String label,
        String description,
        ParamKind kind,
        boolean required,
        String defaultValue,
        String placeholder,
        List<String> choices,
        ParamKind valueKind,
        String entryDelim,
        String pairDelim) {

    public ParamInfo {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("ParamInfo.name must be non-empty");
        if (label == null) label = name;
        if (description == null) description = "";
        if (kind == null) kind = ParamKind.STRING;
        if (choices != null) choices = List.copyOf(choices);
        if (entryDelim == null) entryDelim = ";";
        if (pairDelim == null) pairDelim = "=";
    }

    /// Concise builder for the common case. Required-param variant is
    /// {@link #required(String, ParamKind)}.
    public static Builder of(String name) {
        return new Builder(name);
    }

    public static ParamInfo simple(String name) {
        return new Builder(name).build();
    }

    public static final class Builder {
        private final String name;
        private String label;
        private String description = "";
        private ParamKind kind = ParamKind.STRING;
        private boolean required = false;
        private String defaultValue = null;
        private String placeholder = null;
        private List<String> choices = null;
        private ParamKind valueKind = null;
        private String entryDelim = ";";
        private String pairDelim = "=";

        private Builder(String name) { this.name = name; this.label = name; }

        public Builder label(String v) { this.label = v; return this; }
        public Builder description(String v) { this.description = v == null ? "" : v; return this; }
        public Builder kind(ParamKind v) { this.kind = v; return this; }
        public Builder required() { this.required = true; return this; }
        public Builder required(boolean v) { this.required = v; return this; }
        public Builder defaultValue(String v) { this.defaultValue = v; return this; }
        public Builder placeholder(String v) { this.placeholder = v; return this; }
        public Builder choices(String... v) { this.choices = List.of(v); return this; }
        public Builder valueKind(ParamKind v) { this.valueKind = v; return this; }
        public Builder entryDelim(String v) { this.entryDelim = v; return this; }
        public Builder pairDelim(String v) { this.pairDelim = v; return this; }

        public ParamInfo build() {
            return new ParamInfo(name, label, description, kind, required,
                    defaultValue, placeholder, choices, valueKind, entryDelim, pairDelim);
        }
    }
}
