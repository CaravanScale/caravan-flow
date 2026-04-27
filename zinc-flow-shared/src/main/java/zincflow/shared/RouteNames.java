package zincflow.shared;

/// Paths the UI calls on a worker. Centralised so a rename on the
/// worker side ripples through the UI without hunting for literals.
/// Only the routes the UI actually consumes are listed — full
/// management surface lives in {@code HttpServer} on the worker.
public final class RouteNames {

    public static final String API_IDENTITY     = "/api/identity";
    public static final String API_FLOW         = "/api/flow";
    public static final String API_STATS        = "/api/stats";
    public static final String API_OVERLAYS     = "/api/overlays";
    public static final String API_PROVENANCE_FAILURES = "/api/provenance/failures";
    /// Prefix for the by-FlowFileId lookup — append the id to get the
    /// full path. Matches zinc-flow-csharp's {@code /api/provenance/{id}}.
    public static final String API_PROVENANCE_BY_ID    = "/api/provenance/";
    public static final String API_VC_STATUS    = "/api/vc/status";
    public static final String API_FLOW_SAVE    = "/api/flow/save";

    /// UI-side endpoints the worker may hit (for self-registration).
    public static final String UI_REGISTER      = "/api/registry/register";
    public static final String UI_HEARTBEAT     = "/api/registry/heartbeat";
    public static final String UI_NODES         = "/api/registry/nodes";

    private RouteNames() {}
}
