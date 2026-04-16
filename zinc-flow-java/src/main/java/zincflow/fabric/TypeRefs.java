package zincflow.fabric;

/// Shared type-ref + version-comparison helpers used by every registry
/// (processor, source, provider). Keeps the parse + compare rules in
/// one place so {@code Foo@1.2.3} means exactly the same thing no
/// matter which kind of plugin is being looked up.
public final class TypeRefs {

    public static final String DEFAULT_VERSION = "1.0.0";

    private TypeRefs() {}

    /// Parsed form of a config {@code type:} string — either bare
    /// ({@code "Foo"}, meaning "latest") or pinned ({@code "Foo@1.2.3"}).
    public record TypeRef(String name, String version) {
        public String raw() { return version == null ? name : name + "@" + version; }

        public static TypeRef parse(String raw) {
            if (raw == null) return new TypeRef("", null);
            int at = raw.indexOf('@');
            if (at < 0) return new TypeRef(raw, null);
            return new TypeRef(raw.substring(0, at), raw.substring(at + 1));
        }
    }

    /// Compare two dotted version strings. A trailing {@code ".0"}
    /// difference ({@code "1.2"} vs {@code "1.2.0"}) sorts as equal;
    /// non-numeric segments compare lexicographically.
    public static int compareVersions(String a, String b) {
        String[] as = a.split("\\.");
        String[] bs = b.split("\\.");
        int len = Math.max(as.length, bs.length);
        for (int i = 0; i < len; i++) {
            String ap = i < as.length ? as[i] : "0";
            String bp = i < bs.length ? bs[i] : "0";
            try {
                int cmp = Integer.compare(Integer.parseInt(ap), Integer.parseInt(bp));
                if (cmp != 0) return cmp;
            } catch (NumberFormatException ex) {
                int cmp = ap.compareTo(bp);
                if (cmp != 0) return cmp;
            }
        }
        return 0;
    }

    public static String qualify(String name, String version) {
        return name + "@" + version;
    }
}
