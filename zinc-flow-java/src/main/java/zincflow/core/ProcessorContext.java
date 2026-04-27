package zincflow.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Holds the set of providers available to processors and tracks which
/// processors depend on which providers (for cascade-disable). One
/// context per Fabric instance; passed to {@link zincflow.fabric.Registry}
/// factories so processors can wire the providers they need at
/// construction time.
public final class ProcessorContext {

    private final ConcurrentHashMap<String, Provider> providers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> dependents = new ConcurrentHashMap<>();

    public void addProvider(Provider provider) {
        providers.put(provider.name(), provider);
    }

    public Provider getProvider(String name) {
        return providers.get(name);
    }

    @SuppressWarnings("unchecked")
    public <T extends Provider> T getProviderAs(String name, Class<T> type) {
        Provider p = providers.get(name);
        if (p == null) return null;
        if (!type.isInstance(p)) return null;
        return (T) p;
    }

    public List<String> listProviders() {
        return List.copyOf(providers.keySet());
    }

    public Map<String, Provider> providers() {
        return Map.copyOf(providers);
    }

    public void registerDependent(String providerName, String processorName) {
        dependents.computeIfAbsent(providerName, _ -> new ArrayList<>()).add(processorName);
    }

    public List<String> getDependents(String providerName) {
        List<String> list = dependents.get(providerName);
        return list == null ? List.of() : List.copyOf(list);
    }

    public void shutdownAll() {
        for (Provider p : providers.values()) {
            if (p.isEnabled()) p.disable(60);
            p.shutdown();
        }
    }
}
