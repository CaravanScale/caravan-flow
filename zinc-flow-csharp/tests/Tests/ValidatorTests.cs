using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;

namespace ZincFlow.Tests;

public static class ValidatorTests
{
    public static void RunAll()
    {
        TestEmptyConfigErrors();
        TestNoProcessorsErrors();
        TestUnknownProcessorTypeError();
        TestMissingTypeError();
        TestBrokenConnectionError();
        TestCycleError();
        TestMissingRequiredConfigKey();
        TestMalformedRegexError();
        TestMalformedExpressionTolerated();
        TestValidConfigPasses();
        TestComplexValidConfig();
    }

    private static Registry MakeReg()
    {
        var r = new Registry();
        BuiltinProcessors.RegisterAll(r);
        return r;
    }

    private static Dictionary<string, object?> Config(Dictionary<string, object?> processors)
    {
        return new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = processors
            }
        };
    }

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

    static void TestEmptyConfigErrors()
    {
        Console.WriteLine("--- Validator: empty config errors ---");
        var result = FlowValidator.Validate(new(), MakeReg());
        AssertTrue("empty config has errors", result.HasErrors);
        AssertTrue("error mentions flow section", result.Issues.Any(i => i.Path.Contains("flow")));
    }

    static void TestNoProcessorsErrors()
    {
        Console.WriteLine("--- Validator: no processors errors ---");
        var cfg = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?> { ["processors"] = new Dictionary<string, object?>() }
        };
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("no processors fails", result.HasErrors);
        AssertTrue("error mentions processors", result.Issues.Any(i => i.Path.Contains("flow.processors")));
    }

    static void TestUnknownProcessorTypeError()
    {
        Console.WriteLine("--- Validator: unknown processor type ---");
        var cfg = Config(new() { ["bad"] = Proc("NotARealType") });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("unknown type fails", result.HasErrors);
        AssertTrue("error mentions unknown type",
            result.Issues.Any(i => i.Message.Contains("unknown processor type")));
    }

    static void TestMissingTypeError()
    {
        Console.WriteLine("--- Validator: missing type field ---");
        var def = new Dictionary<string, object?>(); // no type
        var cfg = Config(new() { ["x"] = def });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("missing type fails", result.HasErrors);
    }

    static void TestBrokenConnectionError()
    {
        Console.WriteLine("--- Validator: connection to missing target ---");
        var cfg = Config(new()
        {
            ["a"] = Proc("UpdateAttribute",
                cfg: new() { ["key"] = "k", ["value"] = "v" },
                connections: new() { ["success"] = new() { "ghost" } })
        });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("dangling connection fails", result.HasErrors);
        AssertTrue("error mentions ghost target",
            result.Issues.Any(i => i.Message.Contains("ghost") && i.Message.Contains("not a defined processor")));
    }

    static void TestCycleError()
    {
        Console.WriteLine("--- Validator: cycle detection ---");
        var cfg = Config(new()
        {
            ["a"] = Proc("UpdateAttribute",
                cfg: new() { ["key"] = "k", ["value"] = "v" },
                connections: new() { ["success"] = new() { "b" } }),
            ["b"] = Proc("UpdateAttribute",
                cfg: new() { ["key"] = "k", ["value"] = "v" },
                connections: new() { ["success"] = new() { "a" } })
        });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("cycle fails", result.HasErrors);
        AssertTrue("error mentions cycle", result.Issues.Any(i => i.Message.ToLower().Contains("cycle")));
    }

    static void TestMissingRequiredConfigKey()
    {
        Console.WriteLine("--- Validator: missing required config key ---");
        // UpdateAttribute requires "key" and "value"; omit value → factory throws KeyNotFoundException.
        var cfg = Config(new() { ["x"] = Proc("UpdateAttribute", cfg: new() { ["key"] = "env" }) });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("missing key fails", result.HasErrors);
        AssertTrue("error mentions missing required config",
            result.Issues.Any(i => i.Message.Contains("missing required config key")));
    }

    static void TestMalformedRegexError()
    {
        Console.WriteLine("--- Validator: malformed regex in ReplaceText ---");
        var cfg = Config(new()
        {
            ["x"] = Proc("ReplaceText", cfg: new() { ["pattern"] = "[unclosed", ["replacement"] = "" })
        });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertTrue("bad regex fails", result.HasErrors);
        AssertTrue("error mentions construction failure",
            result.Issues.Any(i => i.Message.Contains("construction failed")));
    }

    static void TestMalformedExpressionTolerated()
    {
        Console.WriteLine("--- Validator: malformed expression in TransformRecord (tolerated) ---");
        // TransformRecord's compute parser swallows FormatException to keep the pipeline alive,
        // so a malformed expression should NOT be reported as a construction error.
        var cfg = Config(new()
        {
            ["x"] = Proc("TransformRecord", cfg: new() { ["operations"] = "compute:total:%%bad%%" })
        });
        var result = FlowValidator.Validate(cfg, MakeReg());
        // No assertion about error count — depending on intent, malformed exprs could go either way.
        // We're documenting the current behavior: no construction error, since the parser is permissive.
        AssertTrue("tolerated (no construction error from TransformRecord)",
            !result.Issues.Any(i => i.Path.Contains("processors.x") && i.Message.Contains("construction failed")));
    }

    static void TestValidConfigPasses()
    {
        Console.WriteLine("--- Validator: valid minimal config passes ---");
        var cfg = Config(new()
        {
            ["tag"] = Proc("UpdateAttribute",
                cfg: new() { ["key"] = "env", ["value"] = "dev" },
                connections: new() { ["success"] = new() { "log" } }),
            ["log"] = Proc("LogAttribute", cfg: new() { ["prefix"] = "flow" })
        });
        var result = FlowValidator.Validate(cfg, MakeReg());
        AssertFalse("valid config has no errors", result.HasErrors);
    }

    static void TestComplexValidConfig()
    {
        Console.WriteLine("--- Validator: complex valid config passes ---");
        var cfg = Config(new()
        {
            ["parse"] = Proc("ConvertJSONToRecord",
                cfg: new() { ["schema_name"] = "orders" },
                connections: new() { ["success"] = new() { "filter" }, ["failure"] = new() { "log" } }),
            ["filter"] = Proc("QueryRecord",
                cfg: new() { ["where"] = "amount > 100" },
                connections: new() { ["success"] = new() { "transform" } }),
            ["transform"] = Proc("TransformRecord",
                cfg: new() { ["operations"] = "compute:total:amount * 1.1" },
                connections: new() { ["success"] = new() { "ocf" } }),
            ["ocf"] = Proc("ConvertRecordToOCF", cfg: new() { ["codec"] = "deflate" }),
            ["log"] = Proc("LogAttribute", cfg: new() { ["prefix"] = "err" })
        });
        var result = FlowValidator.Validate(cfg, MakeReg());
        if (result.HasErrors)
            foreach (var i in result.Issues) Console.WriteLine($"  unexpected: {i}");
        AssertFalse("complex valid config has no errors", result.HasErrors);
    }
}
