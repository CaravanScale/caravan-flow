using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;

namespace ZincFlow.Tests;

public static class EmbeddedSchemaRegistryTests
{
    public static void RunAll()
    {
        TestRegisterAssignsMonotonicId();
        TestRegisterIdenticalReturnsSameId();
        TestGetSubjectLatestAfterMultipleVersions();
        TestGetByIdAfterRegister();
        TestGetUnknownSubjectThrows();
        TestLoadFromConfigInlineAndFile();
        TestLoadFromConfigIdempotent();
        TestLoadFromConfigSkipsBadEntry();
        TestDeleteSubject();
        TestOCFReadViaEmbeddedProvider();
        TestRestEndpointsRoundtrip();
        TestHotReloadUpsert();
        TestConvertOCFAutoRegistersWriterSchema();
        TestConvertOCFAutoRegisterIsIdempotent();
        TestConvertOCFAutoRegisterAddsNewVersion();
        TestConvertOCFAutoRegisterFailureDoesntBlockData();
    }

    private static Schema BuildUserSchema(int extraFields = 0)
    {
        var fields = new List<Field>
        {
            new("id", FieldType.Long),
            new("name", FieldType.String)
        };
        for (int i = 0; i < extraFields; i++)
            fields.Add(new($"extra{i}", FieldType.String));
        return new Schema("User", fields);
    }

    static void TestRegisterAssignsMonotonicId()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: monotonic IDs ---");
        var reg = new EmbeddedSchemaRegistry();
        var id1 = reg.RegisterAsync("a", BuildUserSchema(0)).GetAwaiter().GetResult();
        var id2 = reg.RegisterAsync("b", BuildUserSchema(1)).GetAwaiter().GetResult();
        AssertIntEqual("first id", id1, 1);
        AssertIntEqual("second id", id2, 2);
    }

    static void TestRegisterIdenticalReturnsSameId()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: identical schema dedup ---");
        var reg = new EmbeddedSchemaRegistry();
        var s = BuildUserSchema();
        var id1 = reg.RegisterAsync("users", s).GetAwaiter().GetResult();
        var id2 = reg.RegisterAsync("users", s).GetAwaiter().GetResult();
        AssertIntEqual("same id on re-register", id2, id1);
        AssertIntEqual("version count stays 1", reg.ListVersions("users").Count, 1);
    }

    static void TestGetSubjectLatestAfterMultipleVersions()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: latest tracks newest version ---");
        var reg = new EmbeddedSchemaRegistry();
        reg.RegisterAsync("users", BuildUserSchema(0)).GetAwaiter().GetResult();
        reg.RegisterAsync("users", BuildUserSchema(1)).GetAwaiter().GetResult();
        var (idLatest, latest) = reg.GetSubjectVersionAsync("users", "latest").GetAwaiter().GetResult();
        var (idV1, v1) = reg.GetSubjectVersionAsync("users", "1").GetAwaiter().GetResult();
        AssertIntEqual("latest field count", latest.Fields.Count, 3);
        AssertIntEqual("v1 field count", v1.Fields.Count, 2);
        AssertTrue("latest id != v1 id", idLatest != idV1);
    }

    static void TestGetByIdAfterRegister()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: GetByIdAsync round-trip ---");
        var reg = new EmbeddedSchemaRegistry();
        var id = reg.RegisterAsync("users", BuildUserSchema()).GetAwaiter().GetResult();
        var schema = reg.GetByIdAsync(id).GetAwaiter().GetResult();
        AssertEqual("schema name", schema.Name, "User");
    }

    static void TestGetUnknownSubjectThrows()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: unknown subject throws ---");
        var reg = new EmbeddedSchemaRegistry();
        var threw = false;
        try { reg.GetSubjectVersionAsync("ghost", "latest").GetAwaiter().GetResult(); }
        catch (InvalidOperationException) { threw = true; }
        AssertTrue("unknown subject throws InvalidOperationException", threw);
    }

    static void TestLoadFromConfigInlineAndFile()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: LoadFromConfig (inline + file) ---");
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-sr-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmpDir);
        try
        {
            var fileSchema = "{\"type\":\"record\",\"name\":\"FromFile\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}";
            File.WriteAllText(Path.Combine(tmpDir, "from-file.avsc"), fileSchema);

            var section = new Dictionary<string, object?>
            {
                ["users"] = new Dictionary<string, object?>
                {
                    ["inline"] = "{\"type\":\"record\",\"name\":\"Inline\",\"fields\":[{\"name\":\"y\",\"type\":\"string\"}]}"
                },
                ["orders"] = new Dictionary<string, object?>
                {
                    ["file"] = "from-file.avsc"
                }
            };

            var reg = new EmbeddedSchemaRegistry();
            var loaded = reg.LoadFromConfig(section, tmpDir);
            AssertIntEqual("loaded count", loaded, 2);

            var (_, users) = reg.GetSubjectVersionAsync("users", "latest").GetAwaiter().GetResult();
            AssertEqual("users name", users.Name, "Inline");
            var (_, orders) = reg.GetSubjectVersionAsync("orders", "latest").GetAwaiter().GetResult();
            AssertEqual("orders name", orders.Name, "FromFile");
        }
        finally
        {
            Directory.Delete(tmpDir, recursive: true);
        }
    }

    static void TestLoadFromConfigIdempotent()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: LoadFromConfig is idempotent ---");
        var section = new Dictionary<string, object?>
        {
            ["users"] = new Dictionary<string, object?>
            {
                ["inline"] = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"
            }
        };
        var reg = new EmbeddedSchemaRegistry();
        reg.LoadFromConfig(section, null);
        reg.LoadFromConfig(section, null);
        AssertIntEqual("version count after two loads", reg.ListVersions("users").Count, 1);
    }

    static void TestLoadFromConfigSkipsBadEntry()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: bad entry skipped ---");
        var section = new Dictionary<string, object?>
        {
            ["good"] = new Dictionary<string, object?>
            {
                ["inline"] = "{\"type\":\"record\",\"name\":\"Good\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"
            },
            ["bad"] = new Dictionary<string, object?>
            {
                ["inline"] = "this is not valid json schema"
            },
            ["malformed"] = "expected mapping but I'm a string"
        };
        var reg = new EmbeddedSchemaRegistry();
        var loaded = reg.LoadFromConfig(section, null);
        AssertIntEqual("only good loads", loaded, 1);
        AssertTrue("good is queryable", reg.ListSubjects().Contains("good"));
        AssertFalse("bad is not queryable", reg.ListSubjects().Contains("bad"));
    }

    static void TestDeleteSubject()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: DeleteSubject ---");
        var reg = new EmbeddedSchemaRegistry();
        reg.RegisterAsync("users", BuildUserSchema()).GetAwaiter().GetResult();
        AssertTrue("delete returns true", reg.DeleteSubject("users"));
        AssertFalse("subject gone", reg.ListSubjects().Contains("users"));
        var threw = false;
        try { reg.GetSubjectVersionAsync("users", "latest").GetAwaiter().GetResult(); }
        catch (InvalidOperationException) { threw = true; }
        AssertTrue("get after delete throws", threw);
    }

    static void TestOCFReadViaEmbeddedProvider()
    {
        Console.WriteLine("--- ConvertOCFToRecord: reads via embedded registry provider ---");
        // Pre-load registry with a "users" subject (reader schema)
        var reg = new EmbeddedSchemaRegistry();
        var readerSchema = new Schema("user", [
            new Field("id", FieldType.Long),
            new Field("name", FieldType.String),
            new Field("status", FieldType.String, defaultValue: "unknown")
        ]);
        reg.RegisterAsync("users", readerSchema).GetAwaiter().GetResult();

        // Writer wrote without 'status' — schema evolution fills the default
        var writer = new Schema("user", [
            new Field("id", FieldType.Long),
            new Field("name", FieldType.String)
        ]);
        var rec = new Record(writer);
        rec.SetField("id", 7L);
        rec.SetField("name", "Eve");
        var ocfBytes = new OCFWriter().Write([rec], writer);

        // Wire provider in a context, build processor via factory shape, run
        var srProvider = new SchemaRegistryProvider(reg);
        srProvider.Enable();
        var content = new ContentProvider("content", new MemoryContentStore());
        content.Enable();
        var ctx = new ScopedContext(new Dictionary<string, IProvider>
        {
            ["schema_registry"] = srProvider,
            ["content"] = content
        });

        var proc = new ConvertOCFToRecord(
            store: ((ContentProvider)ctx.GetProvider("content")!).Store,
            staticReaderSchema: null,
            registryProvider: srProvider,
            registrySubject: "users",
            registryVersion: "latest",
            autoRegisterSubject: null);

        var ff = FlowFile.Create(ocfBytes, new());
        var result = (SingleResult)proc.Process(ff);
        var rc = (RecordContent)result.FlowFile.Content;
        AssertIntEqual("decoded record count", rc.Records.Count, 1);
        AssertEqual("name preserved", rc.Records[0].GetField("name")?.ToString() ?? "", "Eve");
        AssertEqual("status defaulted from reader schema", rc.Records[0].GetField("status")?.ToString() ?? "", "unknown");
    }

    static void TestRestEndpointsRoundtrip()
    {
        Console.WriteLine("--- SchemaRegistryHandler: REST round-trip ---");
        var reg = new EmbeddedSchemaRegistry();
        // Pre-load one subject so list/get work without a register first.
        reg.RegisterAsync("preloaded", BuildUserSchema()).GetAwaiter().GetResult();

        var port = FreePort();
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        var app = builder.Build();
        new SchemaRegistryHandler(reg).MapRoutes(app);
        _ = app.RunAsync();
        // Wait for server bind
        for (int i = 0; i < 30; i++)
        {
            try { using var c = new HttpClient { Timeout = TimeSpan.FromMilliseconds(100) }; c.GetAsync($"http://127.0.0.1:{port}/api/schema-registry/subjects").GetAwaiter().GetResult(); break; }
            catch { Thread.Sleep(50); }
        }

        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var baseUrl = $"http://127.0.0.1:{port}/api/schema-registry";

            // GET /subjects
            var subjectsResp = http.GetStringAsync($"{baseUrl}/subjects").GetAwaiter().GetResult();
            AssertTrue("preloaded in GET /subjects", subjectsResp.Contains("preloaded"));

            // POST /subjects/items/versions
            var registerBody = "{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Item\\\",\\\"fields\\\":[{\\\"name\\\":\\\"sku\\\",\\\"type\\\":\\\"string\\\"}]}\"}";
            using var registerReq = new StringContent(registerBody, Encoding.UTF8, "application/vnd.schemaregistry.v1+json");
            var registerResp = http.PostAsync($"{baseUrl}/subjects/items/versions", registerReq).GetAwaiter().GetResult();
            AssertTrue("register ok", registerResp.IsSuccessStatusCode);
            var registerJson = registerResp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            AssertTrue("register returns id", registerJson.Contains("\"id\""));

            // POST same body again → same id (dedup)
            using var registerReq2 = new StringContent(registerBody, Encoding.UTF8, "application/vnd.schemaregistry.v1+json");
            var registerResp2 = http.PostAsync($"{baseUrl}/subjects/items/versions", registerReq2).GetAwaiter().GetResult();
            var registerJson2 = registerResp2.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            AssertEqual("dedup returns same body", registerJson2, registerJson);

            // GET /subjects/items/versions/latest
            var latestResp = http.GetStringAsync($"{baseUrl}/subjects/items/versions/latest").GetAwaiter().GetResult();
            AssertTrue("latest has Item name", latestResp.Contains("Item"));
            AssertTrue("latest has version field", latestResp.Contains("\"version\""));

            // GET /schemas/ids/{id} — extract id from registerJson the lazy way
            var idStart = registerJson.IndexOf("\"id\":") + 5;
            var idEnd = registerJson.IndexOfAny(new[] { ',', '}' }, idStart);
            var id = int.Parse(registerJson.Substring(idStart, idEnd - idStart).Trim());
            var byIdResp = http.GetStringAsync($"{baseUrl}/schemas/ids/{id}").GetAwaiter().GetResult();
            AssertTrue("by-id returns schema field", byIdResp.Contains("\"schema\""));

            // DELETE /subjects/items
            var deleteResp = http.DeleteAsync($"{baseUrl}/subjects/items").GetAwaiter().GetResult();
            AssertTrue("delete ok", deleteResp.IsSuccessStatusCode);

            // GET deleted → 404
            var afterDelete = http.GetAsync($"{baseUrl}/subjects/items/versions/latest").GetAwaiter().GetResult();
            AssertIntEqual("deleted subject 404", (int)afterDelete.StatusCode, 404);

            // GET unknown id → 404
            var unknownId = http.GetAsync($"{baseUrl}/schemas/ids/99999").GetAwaiter().GetResult();
            AssertIntEqual("unknown id 404", (int)unknownId.StatusCode, 404);
        }
        finally
        {
            app.StopAsync().Wait(TimeSpan.FromSeconds(2));
        }
    }

    static void TestHotReloadUpsert()
    {
        Console.WriteLine("--- EmbeddedSchemaRegistry: hot-reload upsert creates new version ---");
        var reg = new EmbeddedSchemaRegistry();
        var section1 = new Dictionary<string, object?>
        {
            ["users"] = new Dictionary<string, object?> { ["inline"] = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}" }
        };
        reg.LoadFromConfig(section1, null);
        var (firstId, _) = reg.GetSubjectVersionAsync("users", "1").GetAwaiter().GetResult();

        // Updated schema for the same subject — adds 'name'.
        var section2 = new Dictionary<string, object?>
        {
            ["users"] = new Dictionary<string, object?> { ["inline"] = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}" }
        };
        reg.LoadFromConfig(section2, null);

        AssertIntEqual("now two versions", reg.ListVersions("users").Count, 2);
        var (latestId, latest) = reg.GetSubjectVersionAsync("users", "latest").GetAwaiter().GetResult();
        AssertTrue("latest id is fresh", latestId != firstId);
        AssertIntEqual("latest has 2 fields", latest.Fields.Count, 2);
        // Old version still queryable
        var (v1Id, _) = reg.GetSubjectVersionAsync("users", "1").GetAwaiter().GetResult();
        AssertIntEqual("v1 id stable", v1Id, firstId);
    }

    static void TestConvertOCFAutoRegistersWriterSchema()
    {
        Console.WriteLine("--- ConvertOCFToRecord: autoRegisterSubject captures writer schema ---");
        var reg = new EmbeddedSchemaRegistry();
        var srProvider = new SchemaRegistryProvider(reg);
        srProvider.Enable();
        var content = new ContentProvider("content", new MemoryContentStore());
        content.Enable();

        // Build an OCF with a known writer schema.
        var writer = new Schema("event", [
            new Field("ts", FieldType.Long),
            new Field("payload", FieldType.String)
        ]);
        var rec = new Record(writer);
        rec.SetField("ts", 1234L);
        rec.SetField("payload", "hello");
        var ocfBytes = new OCFWriter().Write([rec], writer);

        var proc = new ConvertOCFToRecord(
            store: content.Store,
            staticReaderSchema: null,
            registryProvider: srProvider,
            registrySubject: null,
            registryVersion: "latest",
            autoRegisterSubject: "discovered");

        var result = (SingleResult)proc.Process(FlowFile.Create(ocfBytes, new()));
        AssertTrue("decode succeeded", result.FlowFile.Content is RecordContent);

        AssertTrue("registry now has 'discovered'", reg.ListSubjects().Contains("discovered"));
        var (_, captured) = reg.GetSubjectVersionAsync("discovered", "latest").GetAwaiter().GetResult();
        AssertEqual("captured schema name", captured.Name, "event");
        AssertIntEqual("captured field count", captured.Fields.Count, 2);
    }

    static void TestConvertOCFAutoRegisterIsIdempotent()
    {
        Console.WriteLine("--- ConvertOCFToRecord: auto-register is idempotent (dedup) ---");
        var reg = new EmbeddedSchemaRegistry();
        var srProvider = new SchemaRegistryProvider(reg);
        srProvider.Enable();
        var content = new ContentProvider("content", new MemoryContentStore());
        content.Enable();

        var writer = new Schema("event", [new Field("x", FieldType.Long)]);
        var rec = new Record(writer);
        rec.SetField("x", 1L);
        var ocfBytes = new OCFWriter().Write([rec], writer);

        var proc = new ConvertOCFToRecord(content.Store, null, srProvider, null, "latest", "auto");
        proc.Process(FlowFile.Create(ocfBytes, new()));
        proc.Process(FlowFile.Create(ocfBytes, new()));
        proc.Process(FlowFile.Create(ocfBytes, new()));

        AssertIntEqual("only one version after 3 identical processes", reg.ListVersions("auto").Count, 1);
    }

    static void TestConvertOCFAutoRegisterAddsNewVersion()
    {
        Console.WriteLine("--- ConvertOCFToRecord: different writer schemas → new versions ---");
        var reg = new EmbeddedSchemaRegistry();
        var srProvider = new SchemaRegistryProvider(reg);
        srProvider.Enable();
        var content = new ContentProvider("content", new MemoryContentStore());
        content.Enable();

        var writer1 = new Schema("event", [new Field("x", FieldType.Long)]);
        var rec1 = new Record(writer1);
        rec1.SetField("x", 1L);
        var ocf1 = new OCFWriter().Write([rec1], writer1);

        var writer2 = new Schema("event", [new Field("x", FieldType.Long), new Field("y", FieldType.String)]);
        var rec2 = new Record(writer2);
        rec2.SetField("x", 1L);
        rec2.SetField("y", "a");
        var ocf2 = new OCFWriter().Write([rec2], writer2);

        var proc = new ConvertOCFToRecord(content.Store, null, srProvider, null, "latest", "auto");
        proc.Process(FlowFile.Create(ocf1, new()));
        proc.Process(FlowFile.Create(ocf2, new()));

        AssertIntEqual("two versions", reg.ListVersions("auto").Count, 2);
        var (_, latest) = reg.GetSubjectVersionAsync("auto", "latest").GetAwaiter().GetResult();
        AssertIntEqual("latest has 2 fields", latest.Fields.Count, 2);
        var (_, v1) = reg.GetSubjectVersionAsync("auto", "1").GetAwaiter().GetResult();
        AssertIntEqual("v1 has 1 field", v1.Fields.Count, 1);
    }

    static void TestConvertOCFAutoRegisterFailureDoesntBlockData()
    {
        Console.WriteLine("--- ConvertOCFToRecord: auto-register failure doesn't block data ---");
        // Pass null provider but autoRegisterSubject set → the processor's own
        // null-check prevents the call. Data still flows.
        var content = new ContentProvider("content", new MemoryContentStore());
        content.Enable();
        var writer = new Schema("event", [new Field("x", FieldType.Long)]);
        var rec = new Record(writer);
        rec.SetField("x", 1L);
        var ocfBytes = new OCFWriter().Write([rec], writer);

        var proc = new ConvertOCFToRecord(
            store: content.Store,
            staticReaderSchema: null,
            registryProvider: null,
            registrySubject: null,
            registryVersion: "latest",
            autoRegisterSubject: "auto");

        var result = proc.Process(FlowFile.Create(ocfBytes, new()));
        AssertTrue("data flow continues despite missing registry", result is SingleResult);
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
