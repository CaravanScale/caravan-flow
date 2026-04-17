package caravanflow.processors;

import caravanflow.core.ContentResolver;
import caravanflow.core.ContentStore;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Regex capture-group extractor: run {@code pattern} against the FlowFile's
/// raw content, pull out named and positional capture groups, and write each
/// group into a FlowFile attribute. Pure pass-through when the pattern
/// doesn't match — the original FlowFile flows on "success" unchanged.
///
/// Named groups ({@code (?<city>...)}) become attributes named after the
/// group. Positional groups are mapped to attribute names via the
/// {@code groupNames} config (comma-separated, positional — first name
/// maps to group 1, second to group 2, and so on). Empty names in that
/// list skip that group.
///
/// Mirrors caravan-flow-csharp's ExtractText (StdLib/TextProcessors.cs).
public final class ExtractText implements Processor {

    private final Pattern pattern;
    private final List<String> positionalGroupNames;
    private final ContentStore store;

    public ExtractText(String regex, String groupNames, ContentStore store) {
        if (regex == null || regex.isEmpty()) {
            throw new IllegalArgumentException("ExtractText: pattern must not be blank");
        }
        this.pattern = Pattern.compile(regex);
        this.positionalGroupNames = parseGroupNames(groupNames);
        this.store = store;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (ff.content() instanceof RecordContent) {
            return ProcessorResult.failure(
                    "ExtractText: RecordContent not supported — serialize to raw first",
                    ff);
        }

        ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
        if (!resolved.ok()) {
            return ProcessorResult.failure("ExtractText: " + resolved.error(), ff);
        }

        String text = new String(resolved.bytes(), StandardCharsets.UTF_8);
        Matcher matcher = pattern.matcher(text);
        if (!matcher.find()) {
            return ProcessorResult.single(ff);
        }

        FlowFile out = ff;

        // Named groups — detect and lift into attributes.
        for (String name : namedGroups(pattern)) {
            String value = matcher.group(name);
            if (value != null) {
                out = out.withAttribute(name, value);
            }
        }

        // Positional groups — mapped via the groupNames config.
        int available = matcher.groupCount();
        for (int i = 0; i < positionalGroupNames.size() && i < available; i++) {
            String attr = positionalGroupNames.get(i);
            if (attr.isEmpty()) continue;
            String value = matcher.group(i + 1);
            if (value != null) {
                out = out.withAttribute(attr, value);
            }
        }

        return ProcessorResult.single(out);
    }

    private static List<String> parseGroupNames(String spec) {
        if (spec == null || spec.isBlank()) return List.of();
        String[] parts = spec.split(",");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) out.add(p.trim());
        return List.copyOf(out);
    }

    /// Discover named groups by scraping the pattern text — Java's
    /// {@link Pattern} doesn't expose them directly. Handles the
    /// {@code (?<name>...)} syntax, which is the only named-group form
    /// {@link Pattern#compile} accepts.
    private static Set<String> namedGroups(Pattern p) {
        Set<String> names = new LinkedHashSet<>();
        Matcher m = NAMED_GROUP_REGEX.matcher(p.pattern());
        while (m.find()) names.add(m.group(1));
        return names;
    }

    private static final Pattern NAMED_GROUP_REGEX =
            Pattern.compile("\\(\\?<([A-Za-z][A-Za-z0-9]*)>");
}
