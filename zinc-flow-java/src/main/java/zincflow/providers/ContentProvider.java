package zincflow.providers;

import zincflow.core.ComponentState;
import zincflow.core.ContentStore;
import zincflow.core.FileContentStore;
import zincflow.core.MemoryContentStore;
import zincflow.core.Provider;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/// Wraps a {@link ContentStore} as a {@link Provider} so processors can
/// acquire it through the {@link zincflow.core.ProcessorContext} and
/// share one store per Fabric. The provider name is parameterized so
/// multiple stores (in-memory for small-payload pipelines, on-disk for
/// large-payload pipelines) can coexist under distinct names.
public final class ContentProvider implements Provider {

    /// Conventional name — used by processors that don't want to hard-code
    /// a provider name. Matches the C# default.
    public static final String NAME = "content";
    public static final String TYPE = "ContentProvider";

    private final String name;
    private final ContentStore store;
    private volatile ComponentState state = ComponentState.DISABLED;

    public ContentProvider(ContentStore store) {
        this(NAME, store);
    }

    public ContentProvider(String name, ContentStore store) {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("name must not be blank");
        if (store == null) throw new IllegalArgumentException("store must not be null");
        this.name = name;
        this.store = store;
    }

    public ContentStore store() { return store; }

    @Override public String name() { return name; }
    @Override public String providerType() { return TYPE; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }

    /// Build the configured content store: file-backed when
    /// {@code store: file, directory: /path} is set, otherwise in-memory.
    public static final class Plugin implements zincflow.core.ProviderPlugin {
        @Override public String providerType() { return TYPE; }
        @Override public String description() { return "FlowFile content store (memory or disk-backed)."; }
        @Override public List<String> configKeys() { return List.of("store", "directory"); }
        @Override public Provider create(Map<String, Object> config) {
            String kind = config.get("store") == null ? "memory" : String.valueOf(config.get("store"));
            if ("file".equalsIgnoreCase(kind)) {
                Object dir = config.get("directory");
                if (dir == null) throw new IllegalArgumentException(
                        "ContentProvider: file store requires 'directory'");
                try { return new ContentProvider(new FileContentStore(Path.of(String.valueOf(dir)))); }
                catch (java.io.IOException ex) {
                    throw new IllegalStateException(
                            "ContentProvider: failed to init file store at " + dir, ex);
                }
            }
            return new ContentProvider(new MemoryContentStore());
        }
    }
}
