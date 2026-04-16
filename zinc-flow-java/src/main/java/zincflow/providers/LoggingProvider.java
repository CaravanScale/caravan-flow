package zincflow.providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ComponentState;
import zincflow.core.Provider;

import java.util.Map;

/// Structured-logging façade over slf4j. Processors pull a named child
/// logger via {@link #logger(String)} and get the provider's enable-gate
/// for free — logs are suppressed when the provider is disabled, which
/// matches zinc-flow-csharp's "turn off chatty logging in prod" model.
public final class LoggingProvider implements Provider {

    private final Logger rootLogger;
    private volatile ComponentState state = ComponentState.DISABLED;

    public LoggingProvider() {
        this.rootLogger = LoggerFactory.getLogger("zincflow");
    }

    @Override public String name() { return "logging"; }
    @Override public String providerType() { return "logging"; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }

    public Logger logger(String child) {
        return LoggerFactory.getLogger("zincflow." + (child == null ? "app" : child));
    }

    /// Gated info log — drops when disabled. Use for high-volume,
    /// processor-level diagnostics that should be toggle-able.
    public void info(String processor, String msg, Map<String, ?> context) {
        if (!isEnabled()) return;
        rootLogger.info("proc={} msg={} ctx={}", processor, msg, context);
    }

    public void warn(String processor, String msg, Map<String, ?> context) {
        if (!isEnabled()) return;
        rootLogger.warn("proc={} msg={} ctx={}", processor, msg, context);
    }

    public void error(String processor, String msg, Throwable cause) {
        if (!isEnabled()) return;
        rootLogger.error("proc={} msg={}", processor, msg, cause);
    }
}
