package caravanflow.ui;

import org.junit.jupiter.api.Test;
import caravanflow.shared.FlowSnapshot;
import caravanflow.shared.ProcessorView;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class BfsLayoutTest {

    @Test
    void linearFlowProducesOneProcessorPerColumn() {
        FlowSnapshot snap = new FlowSnapshot(
                List.of("a"),
                List.of(proc("a"), proc("b"), proc("c")),
                Map.of("a", Map.of("success", List.of("b")),
                       "b", Map.of("success", List.of("c"))),
                List.of(), List.of(), Map.of());
        BfsLayout layout = BfsLayout.of(snap);
        assertEquals(3, layout.columns().size());
        assertEquals("a", layout.columns().get(0).get(0).name());
        assertEquals("b", layout.columns().get(1).get(0).name());
        assertEquals("c", layout.columns().get(2).get(0).name());
        assertTrue(layout.unreachable().isEmpty());
    }

    @Test
    void branchingFlowPlacesBothChildrenInSameColumn() {
        FlowSnapshot snap = new FlowSnapshot(
                List.of("src"),
                List.of(proc("src"), proc("left"), proc("right"), proc("sink")),
                Map.of("src",   Map.of("success", List.of("left", "right")),
                       "left",  Map.of("success", List.of("sink")),
                       "right", Map.of("success", List.of("sink"))),
                List.of(), List.of(), Map.of());
        BfsLayout layout = BfsLayout.of(snap);
        assertEquals(3, layout.columns().size());
        assertEquals(List.of("left", "right"),
                layout.columns().get(1).stream().map(ProcessorView::name).toList());
        assertEquals("sink", layout.columns().get(2).get(0).name());
    }

    @Test
    void unreachableProcessorLandsInOrphansBucket() {
        FlowSnapshot snap = new FlowSnapshot(
                List.of("a"),
                List.of(proc("a"), proc("lonely")),
                Map.of(), List.of(), List.of(), Map.of());
        BfsLayout layout = BfsLayout.of(snap);
        assertEquals(1, layout.columns().size());
        assertEquals("a", layout.columns().get(0).get(0).name());
        assertEquals(1, layout.unreachable().size());
        assertEquals("lonely", layout.unreachable().get(0).name());
    }

    @Test
    void edgesAreEnumeratedFromTheConnectionsMap() {
        FlowSnapshot snap = new FlowSnapshot(
                List.of("a"),
                List.of(proc("a"), proc("b"), proc("c")),
                Map.of("a", Map.of("success", List.of("b"),
                                   "failure", List.of("c"))),
                List.of(), List.of(), Map.of());
        BfsLayout layout = BfsLayout.of(snap);
        assertEquals(2, layout.edges().size());
        assertTrue(layout.edges().contains(new BfsLayout.Edge("a", "success", "b")));
        assertTrue(layout.edges().contains(new BfsLayout.Edge("a", "failure", "c")));
    }

    @Test
    void emptyFlowProducesEmptyLayout() {
        BfsLayout layout = BfsLayout.of(FlowSnapshot.empty());
        assertTrue(layout.columns().isEmpty());
        assertTrue(layout.unreachable().isEmpty());
        assertTrue(layout.edges().isEmpty());
    }

    private static ProcessorView proc(String name) {
        return new ProcessorView(name, "LogAttribute", "ENABLED", Map.of(), Map.of());
    }
}
