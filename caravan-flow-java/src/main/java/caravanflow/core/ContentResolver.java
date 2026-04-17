package caravanflow.core;

/// Resolve any {@link Content} variant to a raw byte[] so processors
/// that consume content can stay variant-agnostic. RawContent returns
/// its bytes directly; ClaimContent fetches from the store;
/// RecordContent has no byte form and returns an error.
///
/// The tuple-style return (bytes + error string) mirrors the C#
/// {@code ContentHelpers.Resolve}; empty error means success.
public final class ContentResolver {

    private ContentResolver() {}

    public record Resolution(byte[] bytes, String error) {
        public boolean ok() { return error == null || error.isEmpty(); }
    }

    public static Resolution resolve(Content content, ContentStore store) {
        if (content instanceof RawContent raw) {
            return new Resolution(raw.bytes(), "");
        }
        if (content instanceof ClaimContent claim) {
            if (store == null) {
                return new Resolution(new byte[0], "ClaimContent requires a ContentStore but none was provided");
            }
            byte[] out = store.retrieve(claim.claimId());
            return new Resolution(out, "");
        }
        if (content instanceof RecordContent) {
            return new Resolution(new byte[0], "cannot resolve RecordContent to raw bytes — serialize with a record writer first");
        }
        return new Resolution(new byte[0], "unknown content variant: " + content.getClass().getName());
    }
}
