package caravanflow.core;

/// Raw bytes payload. Ownership follows the FlowFile — processors treat
/// the byte[] as effectively immutable; copy-on-mutate if you need a new
/// payload shape (use {@link caravanflow.core.FlowFile#withContent}).
public record RawContent(byte[] bytes) implements Content {

    public RawContent {
        if (bytes == null) {
            throw new IllegalArgumentException("RawContent bytes must not be null — use an empty array instead");
        }
    }

    @Override
    public int size() {
        return bytes.length;
    }
}
