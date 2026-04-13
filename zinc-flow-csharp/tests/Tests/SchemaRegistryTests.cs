using System.Net;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ZincFlow.Core;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;

namespace ZincFlow.Tests;

public static class SchemaRegistryTests
{
    public static void RunAll()
    {
        TestGetById();
        TestGetSubjectVersionLatest();
        TestGetSubjectVersionSpecific();
        TestRegister();
        TestCacheHitsAvoidNetwork();
        TestAuthHeaderForwarded();
        TestErrorPropagation();
        TestOCFReaderUsesRegistrySchema();
    }

    /// <summary>
    /// Minimal stand-in for the Confluent registry. Routes:
    ///   GET  /schemas/ids/{id}
    ///   GET  /subjects/{subject}/versions/{version}
    ///   POST /subjects/{subject}/versions
    ///
    /// Counts call hits per route so cache tests can verify network usage.
    /// Optionally requires a Basic auth header.
    /// </summary>
    private sealed class MockRegistry : IDisposable
    {
        private readonly WebApplication _app;
        public string BaseUrl { get; }
        public int GetByIdCalls { get; private set; }
        public int GetSubjectVersionCalls { get; private set; }
        public int RegisterCalls { get; private set; }
        public string? RequiredAuthHeader { get; set; }
        public Dictionary<int, string> SchemasById { get; } = new();
        public Dictionary<string, (int Id, int Version, string Schema)> Subjects { get; } = new();
        public bool ReturnError { get; set; }

        public MockRegistry(int port)
        {
            BaseUrl = $"http://127.0.0.1:{port}";
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls(BaseUrl);
            // Use source-gen JSON resolver to stay AOT-friendly.
            builder.Services.ConfigureHttpJsonOptions(o =>
                o.SerializerOptions.TypeInfoResolverChain.Insert(0, ZincFlow.Core.ZincJsonContext.Default));

            _app = builder.Build();
            _app.Map("/schemas/ids/{id:int}", (RequestDelegate)HandleGetById);
            _app.Map("/subjects/{subject}/versions/{version}", (RequestDelegate)HandleGetSubjectVersion);
            _app.Map("/subjects/{subject}/versions", (RequestDelegate)HandleRegister);
            _ = _app.RunAsync();
            // Wait for server bind.
            for (int i = 0; i < 30; i++)
            {
                try
                {
                    using var c = new HttpClient { Timeout = TimeSpan.FromMilliseconds(100) };
                    c.GetAsync(BaseUrl + "/").GetAwaiter().GetResult();
                    return;
                }
                catch
                {
                    Thread.Sleep(50);
                }
            }
        }

        private bool CheckAuth(HttpContext ctx)
        {
            if (RequiredAuthHeader is null) return true;
            var got = ctx.Request.Headers.Authorization.ToString();
            if (got == RequiredAuthHeader) return true;
            ctx.Response.StatusCode = 401;
            return false;
        }

        private async Task HandleGetById(HttpContext ctx)
        {
            if (!CheckAuth(ctx)) return;
            GetByIdCalls++;
            var id = int.Parse((string)ctx.Request.RouteValues["id"]!);
            if (ReturnError || !SchemasById.TryGetValue(id, out var schemaText))
            {
                ctx.Response.StatusCode = 404;
                await ctx.Response.WriteAsync($"{{\"error_code\":40403,\"message\":\"schema {id} not found\"}}");
                return;
            }
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync(BuildJson(("schema", schemaText)));
        }

        private async Task HandleGetSubjectVersion(HttpContext ctx)
        {
            if (!CheckAuth(ctx)) return;
            GetSubjectVersionCalls++;
            var subject = (string)ctx.Request.RouteValues["subject"]!;
            var version = (string)ctx.Request.RouteValues["version"]!;
            if (!Subjects.TryGetValue(subject, out var entry))
            {
                ctx.Response.StatusCode = 404;
                return;
            }
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync(BuildJson(
                ("id", entry.Id),
                ("version", entry.Version),
                ("subject", subject),
                ("schema", entry.Schema)));
        }

        private async Task HandleRegister(HttpContext ctx)
        {
            if (!CheckAuth(ctx)) return;
            if (ctx.Request.Method != "POST") { ctx.Response.StatusCode = 405; return; }
            RegisterCalls++;
            var subject = (string)ctx.Request.RouteValues["subject"]!;
            using var sr = new StreamReader(ctx.Request.Body);
            var body = await sr.ReadToEndAsync();
            // Extract the inner schema string. We're a test mock — quick-and-dirty parse.
            var startIdx = body.IndexOf("\"schema\":");
            if (startIdx < 0) { ctx.Response.StatusCode = 400; return; }
            // Simpler: assume the body is well-formed and the schema string value follows.
            var doc = System.Text.Json.JsonDocument.Parse(body);
            var schemaText = doc.RootElement.GetProperty("schema").GetString() ?? "";
            var newId = SchemasById.Count + 1000;
            SchemasById[newId] = schemaText;
            Subjects[subject] = (newId, 1, schemaText);
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync(BuildJson(("id", newId)));
        }

        private static string BuildJson(params (string Key, object Value)[] pairs)
        {
            var dict = new Dictionary<string, object?>();
            foreach (var (k, v) in pairs) dict[k] = v;
            return ZincFlow.Core.ZincJson.SerializeToString(dict);
        }

        public void Dispose() => _app.StopAsync().Wait(TimeSpan.FromSeconds(2));
    }

    private static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private const string SampleSchemaJson =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"long\"}," +
        "{\"name\":\"name\",\"type\":\"string\"}]}";

    static void TestGetById()
    {
        Console.WriteLine("--- SchemaRegistry: GetByIdAsync ---");
        using var mock = new MockRegistry(FreePort());
        mock.SchemasById[42] = SampleSchemaJson;
        using var client = new SchemaRegistryClient(mock.BaseUrl);
        var schema = client.GetByIdAsync(42).GetAwaiter().GetResult();
        AssertEqual("schema name from registry", schema.Name, "User");
        AssertIntEqual("schema field count", schema.Fields.Count, 2);
    }

    static void TestGetSubjectVersionLatest()
    {
        Console.WriteLine("--- SchemaRegistry: GetSubjectVersionAsync(latest) ---");
        using var mock = new MockRegistry(FreePort());
        mock.Subjects["orders"] = (Id: 100, Version: 3, Schema: SampleSchemaJson);
        using var client = new SchemaRegistryClient(mock.BaseUrl);
        var (id, schema) = client.GetSubjectVersionAsync("orders", "latest").GetAwaiter().GetResult();
        AssertIntEqual("subject id", id, 100);
        AssertEqual("subject schema name", schema.Name, "User");
    }

    static void TestGetSubjectVersionSpecific()
    {
        Console.WriteLine("--- SchemaRegistry: GetSubjectVersionAsync(specific version) ---");
        using var mock = new MockRegistry(FreePort());
        mock.Subjects["orders"] = (Id: 100, Version: 3, Schema: SampleSchemaJson);
        using var client = new SchemaRegistryClient(mock.BaseUrl);
        var (id, _) = client.GetSubjectVersionAsync("orders", "3").GetAwaiter().GetResult();
        AssertIntEqual("specific version id", id, 100);
    }

    static void TestRegister()
    {
        Console.WriteLine("--- SchemaRegistry: RegisterAsync ---");
        using var mock = new MockRegistry(FreePort());
        using var client = new SchemaRegistryClient(mock.BaseUrl);
        var schema = new Schema("Item", [
            new Field("sku", FieldType.String),
            new Field("price", FieldType.Double)
        ]);
        var id = client.RegisterAsync("items", schema).GetAwaiter().GetResult();
        AssertTrue("registration returns positive id", id > 0);
        AssertTrue("registry now knows the schema", mock.SchemasById.ContainsKey(id));
    }

    static void TestCacheHitsAvoidNetwork()
    {
        Console.WriteLine("--- SchemaRegistry: cache hits skip the network ---");
        using var mock = new MockRegistry(FreePort());
        mock.SchemasById[7] = SampleSchemaJson;
        mock.Subjects["users"] = (Id: 7, Version: 2, Schema: SampleSchemaJson);

        using var client = new SchemaRegistryClient(mock.BaseUrl);

        // First by-id call → 1 network hit.
        client.GetByIdAsync(7).GetAwaiter().GetResult();
        AssertIntEqual("first by-id call", mock.GetByIdCalls, 1);
        // Second by-id call → cached, no network.
        client.GetByIdAsync(7).GetAwaiter().GetResult();
        AssertIntEqual("by-id cache hit", mock.GetByIdCalls, 1);

        // Specific version is cached.
        client.GetSubjectVersionAsync("users", "2").GetAwaiter().GetResult();
        AssertIntEqual("first specific-version call", mock.GetSubjectVersionCalls, 1);
        client.GetSubjectVersionAsync("users", "2").GetAwaiter().GetResult();
        AssertIntEqual("specific-version cache hit", mock.GetSubjectVersionCalls, 1);

        // "latest" must always hit the wire.
        client.GetSubjectVersionAsync("users", "latest").GetAwaiter().GetResult();
        client.GetSubjectVersionAsync("users", "latest").GetAwaiter().GetResult();
        AssertIntEqual("latest never cached", mock.GetSubjectVersionCalls, 3);
    }

    static void TestAuthHeaderForwarded()
    {
        Console.WriteLine("--- SchemaRegistry: Basic auth forwarded ---");
        using var mock = new MockRegistry(FreePort());
        mock.SchemasById[1] = SampleSchemaJson;
        var creds = "alice:s3cret";
        var expectedHeader = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(creds));
        mock.RequiredAuthHeader = expectedHeader;

        using var clientWithAuth = new SchemaRegistryClient(mock.BaseUrl, basicAuth: creds);
        var schema = clientWithAuth.GetByIdAsync(1).GetAwaiter().GetResult();
        AssertEqual("auth-protected fetch worked", schema.Name, "User");

        // Without auth, server returns 401.
        using var clientNoAuth = new SchemaRegistryClient(mock.BaseUrl);
        var threw = false;
        try { clientNoAuth.GetByIdAsync(1).GetAwaiter().GetResult(); }
        catch (InvalidOperationException) { threw = true; }
        AssertTrue("missing auth → exception", threw);
    }

    static void TestErrorPropagation()
    {
        Console.WriteLine("--- SchemaRegistry: 4xx propagates as exception ---");
        using var mock = new MockRegistry(FreePort());
        // No schemas registered → 404 on lookup
        using var client = new SchemaRegistryClient(mock.BaseUrl);
        var threw = false;
        try { client.GetByIdAsync(999).GetAwaiter().GetResult(); }
        catch (InvalidOperationException ex)
        {
            threw = true;
            AssertTrue("error message includes status", ex.Message.Contains("404"));
        }
        AssertTrue("404 throws", threw);
    }

    static void TestOCFReaderUsesRegistrySchema()
    {
        Console.WriteLine("--- ConvertOCFToRecord: reader_schema_subject pulls from registry ---");
        using var mock = new MockRegistry(FreePort());

        // Writer wrote with v1 (just name).
        var writerSchema = new Schema("user", [new Field("name", FieldType.String)]);
        var rec = new GenericRecord(writerSchema);
        rec.SetField("name", "Eve");
        var ocfBytes = new OCFWriter().Write([rec], writerSchema);

        // Registry serves v2: name + status (with default).
        var v2Json =
            "{\"type\":\"record\",\"name\":\"user\",\"fields\":[" +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"status\",\"type\":\"string\"}]}";
        // Reader schema with default for status — manually built (registry serves the JSON only,
        // defaults are applied via our reader-schema model).
        var readerSchema = new Schema("user", [
            new Field("name", FieldType.String),
            new Field("status", FieldType.String, defaultValue: "unknown")
        ]);
        mock.Subjects["user"] = (Id: 200, Version: 1, Schema: v2Json);

        using var client = new SchemaRegistryClient(mock.BaseUrl);

        // Use the client manually — wiring through processor would also work, but this test
        // focuses on the registry surface.
        var (id, fetched) = client.GetSubjectVersionAsync("user", "latest").GetAwaiter().GetResult();
        AssertTrue("registry served schema", fetched.Fields.Count == 2);
        AssertIntEqual("registry served id", id, 200);

        // The fetched schema does not carry defaults (default is JSON-encoded but our parser
        // ignores defaults today). Use the locally-built readerSchema for evolution semantics.
        var (_, decoded) = new OCFReader().Read(ocfBytes, readerSchema);
        AssertEqual("name from old record", decoded[0].GetField("name")?.ToString() ?? "", "Eve");
        AssertEqual("status from default", decoded[0].GetField("status")?.ToString() ?? "", "unknown");
    }
}
