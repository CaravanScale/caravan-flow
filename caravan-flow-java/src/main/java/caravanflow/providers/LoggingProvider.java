package caravanflow.providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import caravanflow.core.ComponentState;
import caravanflow.core.Provider;

import java.util.Map;

/// Structured-logging façade over slf4j. Processors pull a named child
/// logger via {@link #logger(String)} and get the provider's enable-gate
/// for free — logs are suppressed when the provider is disabled, which
/// matches caravan-flow-csharp's "turn off chatty logging in prod" model.
public final class LoggingProvider implements Provider {

    public static final String NAME = "logging";
    public static final String TYPE = "LoggingProvider";

    private final Logger rootLogger;
    private volatile ComponentState state = ComponentState.DISABLED;

    public LoggingProvider() {
        this.rootLogger = LoggerFactory.getLogger("caravanflow");
    }

    /// Convenience factory — a pre-enabled instance, suitable as the
    /// default when no external provider is wired. The returned
    /// provider logs immediately; toggle it via {@link #disable(int)}
    /// if you want to mute it.
    public static LoggingProvider enabled() {
        LoggingProvider p = new LoggingProvider();
        p.enable();
        return p;
    }

    @Override public String name() { return NAME; }
    @Override public String providerType() { return TYPE; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }

    public Logger logger(String child) {
        return LoggerFactory.getLogger("caravanflow." + (child == null ? "app" : child));
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

    public static final class Plugin implements caravanflow.core.ProviderPlugin {
        @Override public String providerType() { return TYPE; }
        @Override public String description() { return "Structured logging facade over slf4j."; }
        @Override public Provider create(Map<String, Object> config) { return new LoggingProvider(); }
    }
}
