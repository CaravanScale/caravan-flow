package zincflow.processors;

import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.util.ArrayList;
import java.util.List;

/// Routes a FlowFile to a named relationship based on the first matching
/// rule in a rules-spec string. Rules form: {@code "routeA: attr OP value; routeB: attr OP value"}.
///
/// Supported operators (Phase 2 subset): {@code ==}, {@code !=}.
/// The full operator set from zinc-flow-csharp (matches, contains,
/// startsWith, numeric comparisons, etc.) lands in Phase 3.
///
/// No rule matches → routes the FlowFile to "unmatched". Downstream
/// connections decide what happens from there; wire an UpdateAttribute
/// or a sink to "unmatched" to handle the case explicitly.
public final class RouteOnAttribute implements Processor {

    private final List<Rule> rules;

    public RouteOnAttribute(String spec) {
        this.rules = parse(spec);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        for (Rule rule : rules) {
            String attrValue = ff.attributes().get(rule.attribute());
            boolean match = switch (rule.op()) {
                case EQ  -> rule.value().equals(attrValue);
                case NEQ -> !rule.value().equals(attrValue);
            };
            if (match) return ProcessorResult.routed(rule.name(), ff);
        }
        return ProcessorResult.routed("unmatched", ff);
    }

    /// Expose parsed rules for test introspection.
    public List<Rule> rules() {
        return rules;
    }

    // --- Parsing ---

    private static List<Rule> parse(String spec) {
        if (spec == null || spec.isBlank()) return List.of();
        List<Rule> out = new ArrayList<>();
        String[] entries = spec.split(";");
        for (int i = 0; i < entries.length; i++) {
            String entry = entries[i].trim();
            if (entry.isEmpty()) continue;
            int colonIdx = entry.indexOf(':');
            if (colonIdx <= 0) {
                throw new IllegalArgumentException(
                        "RouteOnAttribute: malformed route at index " + i + ": '" + entry
                                + "' — expected 'name: attr OP value'");
            }
            String routeName = entry.substring(0, colonIdx).trim();
            String condition = entry.substring(colonIdx + 1).trim();
            String[] parts = condition.split("\\s+", 3);
            if (parts.length < 3) {
                throw new IllegalArgumentException(
                        "RouteOnAttribute: route '" + routeName + "' has malformed condition: '"
                                + condition + "' — expected 'attr OP value'");
            }
            Op op = switch (parts[1]) {
                case "==" -> Op.EQ;
                case "!=" -> Op.NEQ;
                default -> throw new IllegalArgumentException(
                        "RouteOnAttribute: route '" + routeName + "' has unsupported operator '"
                                + parts[1] + "' — Phase 2 supports '==' and '!='");
            };
            out.add(new Rule(routeName, parts[0], op, parts[2]));
        }
        return List.copyOf(out);
    }

    public enum Op { EQ, NEQ }

    public record Rule(String name, String attribute, Op op, String value) {}
}
