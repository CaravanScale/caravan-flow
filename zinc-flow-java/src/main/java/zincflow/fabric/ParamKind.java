package zincflow.fabric;

/// Kind of a processor parameter — drives UI form rendering and round-trip
/// string encoding. Enum values serialize to JSON as their {@code name()}
/// via Jackson's default enum serializer.
public enum ParamKind {
    STRING,
    MULTILINE,
    INTEGER,
    NUMBER,
    BOOLEAN,
    ENUM,
    EXPRESSION,
    KEY_VALUE_LIST,
    STRING_LIST,
    SECRET;

    /// PascalCase form matching the C# track's JSON output, so the React
    /// UI sees identical kind strings regardless of worker language.
    public String jsonName() {
        return switch (this) {
            case STRING -> "String";
            case MULTILINE -> "Multiline";
            case INTEGER -> "Integer";
            case NUMBER -> "Number";
            case BOOLEAN -> "Boolean";
            case ENUM -> "Enum";
            case EXPRESSION -> "Expression";
            case KEY_VALUE_LIST -> "KeyValueList";
            case STRING_LIST -> "StringList";
            case SECRET -> "Secret";
        };
    }
}
