package zincflow.providers;

import org.junit.jupiter.api.Test;
import zincflow.core.ComponentState;

import static org.junit.jupiter.api.Assertions.*;

final class SchemaRegistryProviderTest {

    @Test
    void registerBumpsVersionPerSubject() {
        var reg = new SchemaRegistryProvider();
        var v1 = reg.register("order", "{\"type\":\"record\",\"v\":1}");
        var v2 = reg.register("order", "{\"type\":\"record\",\"v\":2}");
        assertEquals(1, v1.version());
        assertEquals(2, v2.version());
        assertEquals(2, reg.latest("order").version());
    }

    @Test
    void differentSubjectsAreIndependent() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "{}");
        reg.register("user", "{}");
        reg.register("order", "{}");
        assertEquals(2, reg.latest("order").version());
        assertEquals(1, reg.latest("user").version());
        assertEquals(2, reg.subjects().size());
    }

    @Test
    void getFetchesSpecificVersion() {
        var reg = new SchemaRegistryProvider();
        reg.register("order", "v1");
        reg.register("order", "v2");
        assertEquals("v1", reg.get("order", 1).definition());
        assertEquals("v2", reg.get("order", 2).definition());
        assertNull(reg.get("order", 99));
        assertNull(reg.get("unknown", 1));
    }

    @Test
    void latestReturnsNullWhenSubjectUnknown() {
        assertNull(new SchemaRegistryProvider().latest("ghost"));
    }

    @Test
    void lifecycleTogglesStateAndClearsOnShutdown() {
        var reg = new SchemaRegistryProvider();
        assertEquals(ComponentState.DISABLED, reg.state());
        reg.enable();
        assertEquals(ComponentState.ENABLED, reg.state());

        reg.register("order", "{}");
        assertEquals(1, reg.size());

        reg.disable(0);
        assertEquals(ComponentState.DISABLED, reg.state());
        assertEquals(1, reg.size(), "disable keeps schemas");

        reg.shutdown();
        assertEquals(0, reg.subjects().size(), "shutdown drops every registered schema");
    }

    @Test
    void rejectsBlankArguments() {
        var reg = new SchemaRegistryProvider();
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema("", 1, "{}"));
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema("s", 0, "{}"));
        assertThrows(IllegalArgumentException.class, () ->
                new SchemaRegistryProvider.Schema("s", 1, null));
    }
}
