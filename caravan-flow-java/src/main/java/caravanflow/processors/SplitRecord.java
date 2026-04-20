package caravanflow.processors;

import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Fan out a RecordContent FlowFile into one FlowFile per record. Each
/// output carries the original attributes plus zero-padded {@code split.index}
/// and {@code split.total} attributes so downstream sorting by index is
/// lexically stable.
///
/// This is the dataflow analogue of list unfolding — a stream of records
/// becomes a stream of single-record events that can then be routed,
/// transformed, or sunk independently. Pairs naturally with
/// {@link RouteRecord} for per-record routing.
public final class SplitRecord implements Processor {

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc) || rc.records().isEmpty()) {
            return ProcessorResult.single(ff);
        }

        int total = rc.records().size();
        int width = Integer.toString(total).length();
        String totalStr = Integer.toString(total);

        List<FlowFile> children = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            Map<String, Object> record = rc.records().get(i);
            RecordContent child = new RecordContent(List.of(record), rc.schema());
            FlowFile emitted = ff
                    .withContent(child)
                    .withAttribute("split.index", padLeft(Integer.toString(i), width))
                    .withAttribute("split.total", totalStr);
            children.add(emitted);
        }
        return ProcessorResult.multiple(children);
    }

    private static String padLeft(String s, int width) {
        if (s.length() >= width) return s;
        StringBuilder sb = new StringBuilder(width);
        for (int i = 0; i < width - s.length(); i++) sb.append('0');
        sb.append(s);
        return sb.toString();
    }
}
