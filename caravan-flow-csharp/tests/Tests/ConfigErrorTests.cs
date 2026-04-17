using CaravanFlow.Core;
using CaravanFlow.Fabric;
using CaravanFlow.StdLib;
using static CaravanFlow.Tests.TestRunner;

namespace CaravanFlow.Tests;

/// <summary>
/// Exercises the errors-as-values cleanup: every previously silent-fallback
/// config path must surface as a ConfigException at load time and aggregate
/// into Fabric.LoadFlow's AggregateException (so operators see every problem
/// in one pass). FlowValidator is the convenient path for per-processor
/// assertions; LoadFlow is used for the aggregation test.
/// </summary>
public static class ConfigErrorTests
{
    public static void RunAll()
    {
        Console.WriteLine();
        Console.WriteLine("=== ConfigErrorTests: silent-fallback cleanup ===");

        // Tranche 1 — parse helpers
        TestSplitTextBadHeaderLines();
        TestCsvBadDelimiter();
        TestCsvBadHasHeaderBool();

        // Tranche 2 — malformed-spec throws
        TestRouteOnAttributeMalformedEntry();
        TestRouteOnAttributeUnknownOperator();
        TestExtractRecordFieldMalformedSpec();
        TestAvroUnknownFieldType();
        TestAvroMalformedFieldDef();
        TestQueryRecordMalformedWhere();
        TestQueryRecordUnknownOperator();

        // Tranche 3 — factory vocab + unwired warning
        TestTransformRecordUnknownOp();
        TestTransformRecordMalformedDirective();
        TestEvaluateExpressionMalformedPair();
        TestPutStdoutUnknownFormat();
        TestOcfUnknownCodec();
        TestRouteOnAttributeUnwiredUnmatchedWarns();
        TestRouteOnAttributeWiredUnmatchedNoWarn();

        // Aggregation — all errors reported, not just the first
        TestLoadFlowAggregatesAllErrors();
    }

    // --- helpers ---

    private static Registry Reg()
    {
        var r = new Registry();
        BuiltinProcessors.RegisterAll(r);
        return r;
    }

    private static Dictionary<string, object?> Flow(Dictionary<string, object?> processors)
        => new() { ["flow"] = new Dictionary<string, object?> { ["processors"] = processors } };

    private static Dictionary<string, object?> Proc(string type, Dictionary<string, string>? cfg = null,
        Dictionary<string, List<string>>? connections = null)
    {
        var def = new Dictionary<string, object?> { ["type"] = type };
        if (cfg is not null)
        {
            var c = new Dictionary<string, object?>();
            foreach (var (k, v) in cfg) c[k] = v;
            def["config"] = c;
        }
        if (connections is not null)
        {
            var c = new Dictionary<string, object?>();
            foreach (var (rel, dests) in connections) c[rel] = dests.Cast<object?>().ToList();
            def["connections"] = c;
        }
        return def;
    }

    // Asserts the validator reports an error whose message mentions every
    // fragment. Fragments are matched case-sensitively to keep intent clear.
    private static void AssertConfigError(string label, FlowValidator.Result result,
        string procName, params string[] messageFragments)
    {
        var issue = result.Issues.FirstOrDefault(i =>
            i.Severity == "error" && i.Path.Contains($"processors.{procName}"));
        if (issue is null)
        {
            AssertTrue($"{label} — error raised", false);
            return;
        }
        foreach (var frag in messageFragments)
            AssertTrue($"{label} — message contains '{frag}'", issue.Message.Contains(frag));
    }

    // --- Tranche 1 ---

    static void TestSplitTextBadHeaderLines()
    {
        Console.WriteLine("--- SplitText: header_lines not an int ---");
        var cfg = Flow(new() { ["s"] = Proc("SplitText", new() { ["delimiter"] = ",", ["header_lines"] = "seven" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("splittext bad int", r, "s", "header_lines", "seven");
    }

    static void TestCsvBadDelimiter()
    {
        Console.WriteLine("--- ConvertCSVToRecord: multi-char delimiter ---");
        // ", " (comma+space) used to silently pick the comma; must now error.
        var cfg = Flow(new() { ["c"] = Proc("ConvertCSVToRecord", new() { ["delimiter"] = ", " }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("csv delimiter", r, "c", "delimiter", ", ");
    }

    static void TestCsvBadHasHeaderBool()
    {
        Console.WriteLine("--- ConvertCSVToRecord: has_header is not a bool ---");
        var cfg = Flow(new() { ["c"] = Proc("ConvertCSVToRecord", new() { ["has_header"] = "maybe" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("csv has_header bool", r, "c", "has_header", "maybe");
    }

    // --- Tranche 2 ---

    static void TestRouteOnAttributeMalformedEntry()
    {
        Console.WriteLine("--- RouteOnAttribute: malformed entry (missing colon) ---");
        var cfg = Flow(new() { ["r"] = Proc("RouteOnAttribute", new() { ["routes"] = "no-colon-here" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("route malformed", r, "r", "malformed route", "no-colon-here");
    }

    static void TestRouteOnAttributeUnknownOperator()
    {
        Console.WriteLine("--- RouteOnAttribute: unknown operator ---");
        var cfg = Flow(new() { ["r"] = Proc("RouteOnAttribute",
            new() { ["routes"] = "premium: tier WEIRDOP gold" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("route unknown op", r, "r", "WEIRDOP", "valid:");
    }

    static void TestExtractRecordFieldMalformedSpec()
    {
        Console.WriteLine("--- ExtractRecordField: malformed field:attr spec ---");
        // missing colon → malformed
        var cfg = Flow(new() { ["x"] = Proc("ExtractRecordField", new() { ["fields"] = "justfieldname" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("extract malformed", r, "x", "ExtractRecordField", "justfieldname");
    }

    static void TestAvroUnknownFieldType()
    {
        Console.WriteLine("--- ConvertAvroToRecord: unknown field type in 'fields' ---");
        // `stirng` is a typo of `string` — previously silently fell through to String.
        var cfg = Flow(new() { ["a"] = Proc("ConvertAvroToRecord", new() { ["fields"] = "name:stirng" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("avro unknown type", r, "a", "stirng", "valid:");
    }

    static void TestAvroMalformedFieldDef()
    {
        Console.WriteLine("--- ConvertAvroToRecord: malformed field def (no type) ---");
        var cfg = Flow(new() { ["a"] = Proc("ConvertAvroToRecord", new() { ["fields"] = "onlyfield" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("avro malformed", r, "a", "malformed field def", "onlyfield");
    }

    static void TestQueryRecordMalformedWhere()
    {
        Console.WriteLine("--- QueryRecord: malformed where (no operator) ---");
        var cfg = Flow(new() { ["q"] = Proc("QueryRecord", new() { ["where"] = "justfield" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("query malformed", r, "q", "malformed where", "justfield");
    }

    static void TestQueryRecordUnknownOperator()
    {
        Console.WriteLine("--- QueryRecord: unknown operator ---");
        var cfg = Flow(new() { ["q"] = Proc("QueryRecord", new() { ["where"] = "age WEIRD 5" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("query unknown op", r, "q", "unknown operator");
    }

    // --- Tranche 3 ---

    static void TestTransformRecordUnknownOp()
    {
        Console.WriteLine("--- TransformRecord: unknown op (typo) ---");
        // `upppercase` is a typo of `toUpper` — previously silently fell through.
        var cfg = Flow(new() { ["t"] = Proc("TransformRecord",
            new() { ["operations"] = "upppercase:name" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("transform unknown op", r, "t", "upppercase", "valid:");
    }

    static void TestTransformRecordMalformedDirective()
    {
        Console.WriteLine("--- TransformRecord: malformed directive (no arg) ---");
        var cfg = Flow(new() { ["t"] = Proc("TransformRecord", new() { ["operations"] = "rename" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("transform malformed", r, "t", "malformed directive", "rename");
    }

    static void TestEvaluateExpressionMalformedPair()
    {
        Console.WriteLine("--- EvaluateExpression: malformed expressions pair ---");
        // missing `=`
        var cfg = Flow(new() { ["e"] = Proc("EvaluateExpression", new() { ["expressions"] = "nokey" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("eval malformed", r, "e", "malformed pair", "nokey");
    }

    static void TestPutStdoutUnknownFormat()
    {
        Console.WriteLine("--- PutStdout: format not in the allowed set ---");
        var cfg = Flow(new() { ["p"] = Proc("PutStdout", new() { ["format"] = "xml" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("stdout format", r, "p", "format", "xml");
    }

    static void TestOcfUnknownCodec()
    {
        Console.WriteLine("--- ConvertRecordToOCF: unknown codec ---");
        var cfg = Flow(new() { ["o"] = Proc("ConvertRecordToOCF", new() { ["codec"] = "snappy" }) });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertConfigError("ocf codec", r, "o", "codec", "snappy");
    }

    static void TestRouteOnAttributeUnwiredUnmatchedWarns()
    {
        Console.WriteLine("--- RouteOnAttribute: unwired 'unmatched' → warning ---");
        // Two wired outcomes, no unmatched. Rules configured but the catch-all drops silently.
        var cfg = Flow(new()
        {
            ["r"] = Proc("RouteOnAttribute",
                cfg: new() { ["routes"] = "premium: tier EQ gold; standard: tier EQ silver" },
                connections: new() { ["premium"] = new() { "p-sink" }, ["standard"] = new() { "s-sink" } }),
            ["p-sink"] = Proc("LogAttribute"),
            ["s-sink"] = Proc("LogAttribute"),
        });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertTrue("unwired-unmatched warning raised",
            r.Issues.Any(i => i.Severity == "warning"
                && i.Path.Contains("processors.r.connections")
                && i.Message.Contains("unmatched")));
    }

    static void TestRouteOnAttributeWiredUnmatchedNoWarn()
    {
        Console.WriteLine("--- RouteOnAttribute: wired 'unmatched' → no warning ---");
        var cfg = Flow(new()
        {
            ["r"] = Proc("RouteOnAttribute",
                cfg: new() { ["routes"] = "premium: tier EQ gold" },
                connections: new()
                {
                    ["premium"] = new() { "p-sink" },
                    ["unmatched"] = new() { "default-sink" }
                }),
            ["p-sink"] = Proc("LogAttribute"),
            ["default-sink"] = Proc("LogAttribute"),
        });
        var r = FlowValidator.Validate(cfg, Reg());
        AssertFalse("no unwired-unmatched warning when wired",
            r.Issues.Any(i => i.Severity == "warning"
                && i.Path.Contains("processors.r.connections")
                && i.Message.Contains("unmatched")));
    }

    // --- Aggregation ---

    static void TestLoadFlowAggregatesAllErrors()
    {
        Console.WriteLine("--- LoadFlow: aggregate reports every config error, not just the first ---");
        // Four independent breakages: unknown type, missing required key, bad int,
        // malformed route. All must surface in one AggregateException.
        var cfg = Flow(new()
        {
            ["a"] = Proc("DoesNotExist"),
            ["b"] = Proc("UpdateAttribute", new() { ["key"] = "env" /* value missing */ }),
            ["c"] = Proc("SplitText", new() { ["delimiter"] = ",", ["header_lines"] = "oops" }),
            ["d"] = Proc("RouteOnAttribute", new() { ["routes"] = "bogus" }),
        });

        var reg = Reg();
        var ctx = new ProcessorContext();
        var contentStore = new MemoryContentStore();
        var cp = new ContentProvider("content", contentStore); cp.Enable();
        ctx.AddProvider(cp);
        var log = new LoggingProvider(); log.Enable();
        ctx.AddProvider(log);
        var fab = new Fabric.Fabric(reg, ctx);

        AggregateException? caught = null;
        try { fab.LoadFlow(cfg); }
        catch (AggregateException ex) { caught = ex; }

        AssertTrue("LoadFlow threw AggregateException", caught is not null);
        if (caught is null) return;

        var configErrors = caught.InnerExceptions.OfType<ConfigException>().ToList();
        AssertIntEqual("4 config errors aggregated", configErrors.Count, 4);

        // Each broken processor should appear in exactly one inner exception.
        AssertTrue("aggregate mentions processor a (unknown type)",
            configErrors.Any(e => e.ComponentName == "a" && e.Message.Contains("unknown processor type")));
        AssertTrue("aggregate mentions processor b (missing key)",
            configErrors.Any(e => e.ComponentName == "b" && e.Message.Contains("missing required config key")));
        AssertTrue("aggregate mentions processor c (bad int)",
            configErrors.Any(e => e.ComponentName == "c" && e.Message.Contains("header_lines")));
        AssertTrue("aggregate mentions processor d (malformed route)",
            configErrors.Any(e => e.ComponentName == "d" && e.Message.Contains("malformed route")));
    }
}
