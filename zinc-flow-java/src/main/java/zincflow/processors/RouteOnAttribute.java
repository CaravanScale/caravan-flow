package zincflow.processors;

import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/// Routes a FlowFile to a named relationship based on the first matching
/// rule in a rules-spec string. Rules form:
/// {@code "routeA: attr OP value; routeB: attr OP value"}.
///
/// Supported operators (mirrors zinc-flow-csharp):
/// <ul>
///   <li>{@code ==} / {@code EQ}, {@code !=} / {@code NEQ} — equality</li>
///   <li>{@code CONTAINS}, {@code STARTSWITH}, {@code ENDSWITH} — substring tests</li>
///   <li>{@code MATCHES} — full-string regex</li>
///   <li>{@code EXISTS} — attribute presence (value ignored)</li>
///   <li>{@code GT}, {@code GE}, {@code LT}, {@code LE} — numeric comparison
///       (both sides parsed as doubles; fall back to lexicographic for non-numeric values)</li>
/// </ul>
///
/// No rule matches → routes to "unmatched". Downstream connections
/// decide what happens from there; wire a terminal processor or an
/// UpdateAttribute to "unmatched" to handle the fall-through explicitly.
public final class RouteOnAttribute implements Processor {

    private final List<Rule> rules;

    public RouteOnAttribute(String spec) {
        this.rules = parse(spec);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        for (Rule rule : rules) {
            if (rule.evaluate(ff.attributes())) {
                return ProcessorResult.routed(rule.name(), ff);
            }
        }
        return ProcessorResult.routed("unmatched", ff);
    }

    public List<Rule> rules() { return rules; }

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
            // EXISTS takes no value: "routeA: attr EXISTS" is valid.
            String[] parts = condition.split("\\s+", 3);
            if (parts.length < 2) {
                throw new IllegalArgumentException(
                        "RouteOnAttribute: route '" + routeName + "' has malformed condition: '"
                                + condition + "' — expected 'attr OP [value]'");
            }
            Op op = Op.parse(parts[1], routeName);
            String value = parts.length >= 3 ? parts[2] : "";
            if (op != Op.EXISTS && parts.length < 3) {
                throw new IllegalArgumentException(
                        "RouteOnAttribute: route '" + routeName + "' operator " + op
                                + " requires a value but none was provided");
            }
            out.add(new Rule(routeName, parts[0], op, value));
        }
        return List.copyOf(out);
    }

    public enum Op {
        EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, MATCHES, EXISTS, GT, GE, LT, LE;

        static Op parse(String token, String routeName) {
            return switch (token.toUpperCase(Locale.ROOT)) {
                case "==", "EQ" -> EQ;
                case "!=", "NEQ" -> NEQ;
                case "CONTAINS" -> CONTAINS;
                case "STARTSWITH" -> STARTSWITH;
                case "ENDSWITH" -> ENDSWITH;
                case "MATCHES" -> MATCHES;
                case "EXISTS" -> EXISTS;
                case ">", "GT" -> GT;
                case ">=", "GE" -> GE;
                case "<", "LT" -> LT;
                case "<=", "LE" -> LE;
                default -> throw new IllegalArgumentException(
                        "RouteOnAttribute: route '" + routeName + "' has unsupported operator '"
                                + token + "' — valid: EQ/==, NEQ/!=, CONTAINS, STARTSWITH, ENDSWITH, MATCHES, EXISTS, GT/>, GE/>=, LT/<, LE/<=");
            };
        }
    }

    public record Rule(String name, String attribute, Op op, String value) {
        boolean evaluate(java.util.Map<String, String> attributes) {
            if (op == Op.EXISTS) {
                return attributes.containsKey(attribute);
            }
            String actual = attributes.get(attribute);
            if (actual == null) return false;
            return switch (op) {
                case EQ -> value.equals(actual);
                case NEQ -> !value.equals(actual);
                case CONTAINS -> actual.contains(value);
                case STARTSWITH -> actual.startsWith(value);
                case ENDSWITH -> actual.endsWith(value);
                case MATCHES -> actual.matches(value);
                case GT, GE, LT, LE -> compareTo(actual, value, op);
                case EXISTS -> true; // unreachable — handled above
            };
        }

        /// Numeric comparison when both sides parse as doubles, lexicographic
        /// fallback otherwise. Matches the zinc-flow-csharp semantics: string
        /// compare when values aren't parseable as numbers, so GT/LT can be
        /// used on version strings, timestamps-as-ISO-strings, etc.
        private static boolean compareTo(String actual, String expected, Op op) {
            int cmp;
            try {
                double a = Double.parseDouble(actual);
                double b = Double.parseDouble(expected);
                cmp = Double.compare(a, b);
            } catch (NumberFormatException ignored) {
                cmp = actual.compareTo(expected);
            }
            return switch (op) {
                case GT -> cmp > 0;
                case GE -> cmp >= 0;
                case LT -> cmp < 0;
                case LE -> cmp <= 0;
                default -> false; // unreachable
            };
        }
    }
}
