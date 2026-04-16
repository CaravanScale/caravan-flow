package zincflow.providers;

import zincflow.core.ComponentState;
import zincflow.core.ContentStore;
import zincflow.core.Provider;

/// Wraps a {@link ContentStore} as a {@link Provider} so processors can
/// acquire it through the {@link zincflow.core.ProcessorContext} and
/// share one store per Fabric. The provider name is parameterized so
/// multiple stores (in-memory for small-payload pipelines, on-disk for
/// large-payload pipelines) can coexist under distinct names.
public final class ContentProvider implements Provider {

    /// Conventional name — used by processors that don't want to hard-code
    /// a provider name. Matches the C# default.
    public static final String DEFAULT_NAME = "content";

    private final String name;
    private final ContentStore store;
    private volatile ComponentState state = ComponentState.DISABLED;

    public ContentProvider(ContentStore store) {
        this(DEFAULT_NAME, store);
    }

    public ContentProvider(String name, ContentStore store) {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("name must not be blank");
        if (store == null) throw new IllegalArgumentException("store must not be null");
        this.name = name;
        this.store = store;
    }

    public ContentStore store() { return store; }

    @Override public String name() { return name; }
    @Override public String providerType() { return "content"; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }
}
