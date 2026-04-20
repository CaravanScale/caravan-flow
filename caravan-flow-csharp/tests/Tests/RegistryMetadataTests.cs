using CaravanFlow.Core;
using CaravanFlow.Fabric;
using static CaravanFlow.Tests.TestRunner;

namespace CaravanFlow.Tests;

public static class RegistryMetadataTests
{
    public static void RunAll()
    {
        TestEveryBuiltinHasCategoryAndParams();
        TestEnumParamsHaveChoices();
        TestKeyValueListHasDelimsAndValueKind();
        TestLegacyConstructorStillWorks();
        TestJsonShapeForSampleProcessor();
    }

    static Registry BuiltinRegistry()
    {
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        return reg;
    }

    static void TestEveryBuiltinHasCategoryAndParams()
    {
        Console.WriteLine("--- RegistryMetadata: every builtin has non-Other category + coherent params ---");
        var infos = BuiltinRegistry().List();
        AssertTrue("registry is non-empty", infos.Count > 0);
        foreach (var info in infos)
        {
            AssertTrue($"{info.Name}: category is not 'Other'", info.Category != "Other");
            AssertTrue($"{info.Name}: ConfigKeys derives from Parameters",
                info.ConfigKeys.Count == info.Parameters.Count);
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var p in info.Parameters)
            {
                AssertTrue($"{info.Name}.{p.Name}: name non-empty", p.Name.Length > 0);
                AssertTrue($"{info.Name}.{p.Name}: unique", seen.Add(p.Name));
            }
        }
    }

    static void TestEnumParamsHaveChoices()
    {
        Console.WriteLine("--- RegistryMetadata: Enum params have Choices, non-Enum don't need them ---");
        foreach (var info in BuiltinRegistry().List())
        {
            foreach (var p in info.Parameters)
            {
                if (p.Kind == ParamKind.Enum)
                {
                    AssertTrue($"{info.Name}.{p.Name}: Enum has Choices",
                        p.Choices is { Count: > 0 });
                    if (p.Default is not null && p.Choices is not null)
                        AssertTrue($"{info.Name}.{p.Name}: default '{p.Default}' is in choices",
                            p.Choices.Contains(p.Default));
                }
            }
        }
    }

    static void TestKeyValueListHasDelimsAndValueKind()
    {
        Console.WriteLine("--- RegistryMetadata: KeyValueList carries delimiters (and usually valueKind) ---");
        foreach (var info in BuiltinRegistry().List())
        {
            foreach (var p in info.Parameters)
            {
                if (p.Kind == ParamKind.KeyValueList)
                {
                    AssertTrue($"{info.Name}.{p.Name}: entry delim non-empty", p.EntryDelim.Length > 0);
                    AssertTrue($"{info.Name}.{p.Name}: pair delim non-empty", p.PairDelim.Length > 0);
                }
            }
        }
    }

    static void TestLegacyConstructorStillWorks()
    {
        Console.WriteLine("--- RegistryMetadata: legacy ProcessorInfo(name, desc, List<string>) compat ---");
        var info = new ProcessorInfo("X", "test", new List<string> { "a", "b" });
        AssertEqual("category is Other", info.Category, "Other");
        AssertTrue("two parameters", info.Parameters.Count == 2);
        AssertEqual("param 0 name", info.Parameters[0].Name, "a");
        AssertTrue("param 0 kind is String", info.Parameters[0].Kind == ParamKind.String);
        AssertEqual("param 0 label defaults to name", info.Parameters[0].Label, "a");
        AssertTrue("ConfigKeys derived", info.ConfigKeys.Count == 2 && info.ConfigKeys[0] == "a");
    }

    static void TestJsonShapeForSampleProcessor()
    {
        Console.WriteLine("--- RegistryMetadata: JSON-serializable shape for a sample processor (RouteRecord) ---");
        var routeInfo = BuiltinRegistry().GetInfo("RouteRecord");
        AssertTrue("RouteRecord registered", routeInfo is not null);
        AssertEqual("RouteRecord category", routeInfo!.Category, "Routing");
        AssertTrue("RouteRecord has routes param", routeInfo.Parameters.Count == 1);
        var routes = routeInfo.Parameters[0];
        AssertEqual("routes.name", routes.Name, "routes");
        AssertTrue("routes.kind is KeyValueList", routes.Kind == ParamKind.KeyValueList);
        AssertTrue("routes.required", routes.Required);
        AssertTrue("routes.valueKind is Expression",
            routes.ValueKind.HasValue && routes.ValueKind.Value == ParamKind.Expression);
        AssertEqual("routes.entryDelim", routes.EntryDelim, ";");
        AssertEqual("routes.pairDelim", routes.PairDelim, ":");
        AssertTrue("routes has placeholder", !string.IsNullOrEmpty(routes.Placeholder));
    }
}
