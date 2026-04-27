package zincflow.core;

/// Conventional relationship names for {@link ProcessorResult.Routed}
/// and processor registration metadata. Centralised so a rename ripples
/// in one place, and so config validation can cross-check connection
/// keys against a known set.
///
/// Not every processor uses every name — the set here is the NiFi-ish
/// vocabulary the built-in processors share. Plugin processors are
/// free to declare their own relationship names.
public final class Relationships {

    public static final String SUCCESS = "success";
    public static final String FAILURE = "failure";
    public static final String MATCHED = "matched";
    public static final String UNMATCHED = "unmatched";
    public static final String ORIGINAL = "original";

    private Relationships() {}
}
