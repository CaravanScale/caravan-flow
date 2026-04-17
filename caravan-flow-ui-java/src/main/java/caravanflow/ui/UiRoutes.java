package caravanflow.ui;

/// UI-side page paths. Centralised so redirects and nav links point
/// at one source of truth.
public final class UiRoutes {

    public static final String ROOT          = "/";
    public static final String HEALTH        = "/health";
    public static final String FLOW          = "/flow";
    public static final String FLOW_STATS    = "/flow/stats.json";
    public static final String FLOW_PANEL    = "/flow/panel/{name}";
    public static final String LINEAGE          = "/lineage";
    public static final String LINEAGE_LIST     = "/lineage/list";
    public static final String LINEAGE_ONE      = "/lineage/{id}";
    public static final String LINEAGE_ONE_EVENTS = "/lineage/{id}/events";
    public static final String NODES         = "/nodes";
    public static final String SETTINGS      = "/settings";

    private UiRoutes() {}
}
